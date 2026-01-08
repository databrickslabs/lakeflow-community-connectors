# **Google Maps API Documentation**

## **Authorization**

### Preferred Method: API Key

Google Maps APIs use API Key authentication. The API key is passed as a query parameter or header with each request.

**Authentication Parameters:**
| Parameter | Location | Required | Description |
|-----------|----------|----------|-------------|
| `api_key` | Query parameter | Yes | Your Google Maps Platform API key |

**Alternative:** The API key can also be passed in the `X-Goog-Api-Key` header (for Places API New).

**How to obtain an API key:**
1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the required APIs from the API Library:
   - "Places API" or "Places API (New)" for places data
   - "Geocoding API" for geocoder data
   - "Distance Matrix API" for distance matrix data
4. Navigate to "Credentials" and create an API key
5. (Recommended) Restrict the API key to only the required APIs

**Example API Request with Authentication (Places API):**

```bash
# Using header for Places API (New)
curl "https://places.googleapis.com/v1/places:searchText" \
  -H "Content-Type: application/json" \
  -H "X-Goog-Api-Key: YOUR_API_KEY" \
  -H "X-Goog-FieldMask: places.id,places.displayName,places.formattedAddress" \
  -d '{
    "textQuery": "restaurants in New York"
  }'
```

**Example API Request with Authentication (Geocoding API):**

```bash
# Using query parameter for Geocoding API
curl "https://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&key=YOUR_API_KEY"
```

**Important Notes:**
- API keys should be kept secure and not exposed in client-side code
- Enable billing on your Google Cloud project (required for Google Maps APIs)
- Set up API key restrictions to prevent unauthorized usage

---

## **Object List**

The object list for this connector is **static**. This connector supports the following objects:

| Object Name | Description | API Endpoint |
|-------------|-------------|--------------|
| `places` | Location data including businesses, points of interest, and geographic locations | Text Search (New) API |
| `geocoder` | Address-to-coordinates conversion (geocoding) and coordinates-to-address conversion (reverse geocoding) | Geocoding API |
| `distance_matrix` | Travel distance and time calculations between multiple origins and destinations | Distance Matrix API |

### Places Object Description

The `places` object represents location data from Google's database of over 200 million places. Each place record includes:
- Unique place identifier
- Name and address information
- Geographic coordinates
- Business information (hours, ratings, reviews)
- Contact information
- Photos and other rich data

### Geocoder Object Description

The `geocoder` object provides geocoding results that convert addresses into geographic coordinates (latitude/longitude) or reverse geocode coordinates into human-readable addresses. Each geocoder result includes:
- Formatted address string
- Address components (street, city, state, country, postal code)
- Geographic coordinates (latitude/longitude)
- Location type accuracy indicator
- Viewport bounds for displaying the location
- Place ID for cross-referencing with Places API
- Plus Code (Open Location Code)

### Distance Matrix Object Description

The `distance_matrix` object provides travel distance and time for a matrix of origins and destinations. Each element in the response matrix contains:
- Travel distance (in meters and human-readable text)
- Travel duration (in seconds and human-readable text)
- Duration in traffic (for driving mode with traffic conditions)
- Transit fare information (for transit mode)
- Element status indicating if the route was found
- Origin and destination addresses as interpreted by the API

---

## **Object Schema**

The schema for the `places` object is **static** and defined by Google's Place Data Fields. The Places API (New) uses field masks to specify which fields to return.

### Places Object Schema

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique identifier for the place (Place ID) |
| `displayName` | object | The localized name of the place. Contains `text` and `languageCode` |
| `displayName.text` | string | The place's name |
| `displayName.languageCode` | string | Language code of the name |
| `formattedAddress` | string | Human-readable address |
| `shortFormattedAddress` | string | Short formatted address |
| `addressComponents` | array | Array of address component objects |
| `location` | object | Geographic coordinates |
| `location.latitude` | number | Latitude in degrees |
| `location.longitude` | number | Longitude in degrees |
| `viewport` | object | Recommended viewport for displaying the place |
| `googleMapsUri` | string | URL to the place on Google Maps |
| `websiteUri` | string | Website URL of the place |
| `internationalPhoneNumber` | string | Phone number in international format |
| `nationalPhoneNumber` | string | Phone number in national format |
| `types` | array[string] | Place types (e.g., "restaurant", "cafe") |
| `primaryType` | string | The primary type of the place |
| `primaryTypeDisplayName` | object | Localized display name of the primary type |
| `businessStatus` | string | Operational status: `OPERATIONAL`, `CLOSED_TEMPORARILY`, `CLOSED_PERMANENTLY` |
| `priceLevel` | string | Price level: `PRICE_LEVEL_FREE`, `PRICE_LEVEL_INEXPENSIVE`, `PRICE_LEVEL_MODERATE`, `PRICE_LEVEL_EXPENSIVE`, `PRICE_LEVEL_VERY_EXPENSIVE` |
| `rating` | number | Average user rating (1.0 to 5.0) |
| `userRatingCount` | integer | Total number of user ratings |
| `currentOpeningHours` | object | Current week's opening hours |
| `regularOpeningHours` | object | Regular weekly opening hours |
| `utcOffsetMinutes` | integer | UTC offset in minutes |
| `reviews` | array | Array of user review objects |
| `photos` | array | Array of photo objects |
| `editorialSummary` | object | AI-generated editorial summary |
| `iconMaskBaseUri` | string | Base URI for the place icon mask |
| `iconBackgroundColor` | string | Background color for the icon (hex) |
| `takeout` | boolean | Whether takeout is available |
| `delivery` | boolean | Whether delivery is available |
| `dineIn` | boolean | Whether dine-in is available |
| `reservable` | boolean | Whether reservations are supported |
| `servesBreakfast` | boolean | Whether breakfast is served |
| `servesLunch` | boolean | Whether lunch is served |
| `servesDinner` | boolean | Whether dinner is served |
| `servesBeer` | boolean | Whether beer is served |
| `servesWine` | boolean | Whether wine is served |
| `servesBrunch` | boolean | Whether brunch is served |
| `servesVegetarianFood` | boolean | Whether vegetarian food is served |
| `outdoorSeating` | boolean | Whether outdoor seating is available |
| `liveMusic` | boolean | Whether live music is available |
| `menuForChildren` | boolean | Whether a children's menu is available |
| `goodForChildren` | boolean | Whether the place is good for children |
| `allowsDogs` | boolean | Whether dogs are allowed |
| `goodForGroups` | boolean | Whether the place is good for groups |
| `goodForWatchingSports` | boolean | Whether the place is good for watching sports |
| `accessibilityOptions` | object | Accessibility features |
| `parkingOptions` | object | Parking availability information |
| `paymentOptions` | object | Accepted payment methods |
| `plusCode` | object | Plus Code location reference |
| `adrFormatAddress` | string | Address in adr microformat |

### Address Components Schema

| Field Name | Type | Description |
|------------|------|-------------|
| `longText` | string | Full text description of the component |
| `shortText` | string | Abbreviated text |
| `types` | array[string] | Component types (e.g., "locality", "country") |
| `languageCode` | string | Language code |

### Opening Hours Schema

| Field Name | Type | Description |
|------------|------|-------------|
| `openNow` | boolean | Whether currently open |
| `periods` | array | Array of opening periods |
| `weekdayDescriptions` | array[string] | Localized opening hours strings |

---

### Geocoder Object Schema

The schema for the `geocoder` object is **static** and defined by Google's Geocoding API response format.

| Field Name | Type | Description |
|------------|------|-------------|
| `place_id` | string | Unique identifier that can be used with Places API |
| `formatted_address` | string | Human-readable address string |
| `address_components` | array | Array of address component objects |
| `geometry` | object | Location geometry information |
| `geometry.location` | object | Geographic coordinates |
| `geometry.location.lat` | number | Latitude in degrees |
| `geometry.location.lng` | number | Longitude in degrees |
| `geometry.location_type` | string | Accuracy indicator: `ROOFTOP`, `RANGE_INTERPOLATED`, `GEOMETRIC_CENTER`, `APPROXIMATE` |
| `geometry.viewport` | object | Recommended viewport bounds |
| `geometry.viewport.northeast` | object | Northeast corner (lat, lng) |
| `geometry.viewport.southwest` | object | Southwest corner (lat, lng) |
| `geometry.bounds` | object | (Optional) Bounding box enclosing the result |
| `types` | array[string] | Address type indicators (e.g., "street_address", "locality") |
| `partial_match` | boolean | Indicates if the geocoder did not find an exact match |
| `plus_code` | object | Plus Code (Open Location Code) for the location |
| `plus_code.global_code` | string | Global Plus Code (e.g., "849VCWC8+R9") |
| `plus_code.compound_code` | string | Compound Plus Code with locality (e.g., "CWC8+R9 Mountain View, CA") |
| `postcode_localities` | array[string] | (Optional) Localities contained in a postal code |

### Geocoder Address Components Schema

| Field Name | Type | Description |
|------------|------|-------------|
| `long_name` | string | Full text description of the component |
| `short_name` | string | Abbreviated text (e.g., "CA" for California) |
| `types` | array[string] | Component types indicating the address part |

**Common Address Component Types:**
| Type | Description |
|------|-------------|
| `street_number` | Street number |
| `route` | Street name |
| `locality` | City or town |
| `administrative_area_level_1` | State or province |
| `administrative_area_level_2` | County |
| `country` | Country |
| `postal_code` | ZIP or postal code |
| `sublocality` | Neighborhood or district |
| `premise` | Building name |
| `subpremise` | Unit, suite, or floor |

### Geocoder Location Type Enum

| Value | Description |
|-------|-------------|
| `ROOFTOP` | Precise geocode with street address precision |
| `RANGE_INTERPOLATED` | Approximate location interpolated between two precise points |
| `GEOMETRIC_CENTER` | Geometric center of a result (e.g., polyline or polygon) |
| `APPROXIMATE` | Approximate location (e.g., city center) |

### Geocoder Status Codes

| Status | Description |
|--------|-------------|
| `OK` | Request successful, at least one result returned |
| `ZERO_RESULTS` | Geocode successful but no results found |
| `OVER_DAILY_LIMIT` | API key missing, invalid, or billing not enabled |
| `OVER_QUERY_LIMIT` | Quota exceeded |
| `REQUEST_DENIED` | Request denied (check API key permissions) |
| `INVALID_REQUEST` | Query (address, components, or latlng) is missing |
| `UNKNOWN_ERROR` | Server error, retry may succeed |

---

### Distance Matrix Object Schema

The schema for the `distance_matrix` object is **static** and defined by Google's Distance Matrix API response format. The response is a matrix where each row corresponds to an origin and each element in the row corresponds to a destination.

**Top-Level Response Schema:**

| Field Name | Type | Description |
|------------|------|-------------|
| `status` | string | Top-level status of the request |
| `origin_addresses` | array[string] | Addresses as interpreted by the API for each origin |
| `destination_addresses` | array[string] | Addresses as interpreted by the API for each destination |
| `rows` | array | Array of row objects, one per origin |

**Row Schema:**

| Field Name | Type | Description |
|------------|------|-------------|
| `elements` | array | Array of element objects, one per destination |

**Element Schema (each origin-destination pair):**

| Field Name | Type | Description |
|------------|------|-------------|
| `status` | string | Element status: `OK`, `NOT_FOUND`, `ZERO_RESULTS` |
| `distance` | object | Travel distance for this pair |
| `distance.value` | integer | Distance in meters |
| `distance.text` | string | Human-readable distance (e.g., "12.3 km") |
| `duration` | object | Travel duration for this pair |
| `duration.value` | integer | Duration in seconds |
| `duration.text` | string | Human-readable duration (e.g., "15 mins") |
| `duration_in_traffic` | object | (Optional) Duration considering current traffic |
| `duration_in_traffic.value` | integer | Duration in seconds with traffic |
| `duration_in_traffic.text` | string | Human-readable duration with traffic |
| `fare` | object | (Optional) Transit fare, only for transit mode |
| `fare.currency` | string | ISO 4217 currency code |
| `fare.value` | number | Fare amount in local currency |
| `fare.text` | string | Human-readable fare (e.g., "$2.50") |

### Distance Matrix Status Codes (Top-Level)

| Status | Description |
|--------|-------------|
| `OK` | Response contains valid results |
| `INVALID_REQUEST` | The provided request was invalid |
| `MAX_ELEMENTS_EXCEEDED` | Product of origins and destinations exceeds per-query limit |
| `MAX_DIMENSIONS_EXCEEDED` | Request contains more than 25 origins or 25 destinations |
| `OVER_DAILY_LIMIT` | API key missing, invalid, or billing not enabled |
| `OVER_QUERY_LIMIT` | Too many requests within allowed time period |
| `REQUEST_DENIED` | Service denied for your application |
| `UNKNOWN_ERROR` | Server error, retry may succeed |

### Distance Matrix Element Status Codes

| Status | Description |
|--------|-------------|
| `OK` | Valid result for this origin-destination pair |
| `NOT_FOUND` | Origin and/or destination could not be geocoded |
| `ZERO_RESULTS` | No route could be found between origin and destination |

### Travel Mode Enum

| Value | Description |
|-------|-------------|
| `driving` | Driving directions (default) |
| `walking` | Walking directions via pedestrian paths |
| `bicycling` | Bicycling directions via bike paths |
| `transit` | Public transit directions |

### Traffic Model Enum (for driving with departure_time)

| Value | Description |
|-------|-------------|
| `best_guess` | Best estimate based on historical and live traffic (default) |
| `pessimistic` | Longer than typical duration |
| `optimistic` | Shorter than typical duration |

### Avoid Options

| Value | Description |
|-------|-------------|
| `tolls` | Avoid toll roads |
| `highways` | Avoid highways |
| `ferries` | Avoid ferries |
| `indoor` | Avoid indoor steps (walking/transit) |

### Unit System Enum

| Value | Description |
|-------|-------------|
| `metric` | Distances in kilometers and meters |
| `imperial` | Distances in miles and feet |

---

## **Get Object Primary Keys**

The primary keys for objects are **static** and do not require an API call.

| Object | Primary Key Column | Type | Description |
|--------|-------------------|------|-------------|
| `places` | `id` | string | The Place ID - a unique identifier that represents a specific place in the Google Places database |
| `geocoder` | `place_id` | string | The Place ID returned by geocoding - uniquely identifies the geocoded location |
| `distance_matrix` | `origin_index`, `destination_index` | integer, integer | Composite key: row index (origin) and element index (destination) within the response matrix |

**Place ID Characteristics:**
- Globally unique identifier
- Stable over time (though may occasionally change)
- Can be used to retrieve place details directly
- Format: Alphanumeric string (e.g., `ChIJN1t_tDeuEmsRUsoyG83frY4`)
- Shared across Places and Geocoding APIs - can be used to cross-reference data

**Distance Matrix Key Characteristics:**
- Composite key based on position in the matrix
- `origin_index` corresponds to the position in the `origins` request parameter
- `destination_index` corresponds to the position in the `destinations` request parameter
- Results can also be keyed by the `origin_addresses` and `destination_addresses` returned in the response

---

## **Object's Ingestion Type**

| Object | Ingestion Type | Rationale |
|--------|---------------|-----------|
| `places` | `snapshot` | The Places API does not provide change feeds, cursors, or timestamps for incremental sync. Each query returns the current state of matching places. Place data can change (ratings, hours, etc.) but there's no mechanism to track changes over time. |
| `geocoder` | `snapshot` | The Geocoding API is a query-based service that returns results for specific address/coordinate inputs. There is no change feed or incremental sync mechanism. Each request returns current geocoding data. |
| `distance_matrix` | `snapshot` | The Distance Matrix API is a query-based service that calculates distances/durations for specific origin-destination pairs. Results reflect real-time traffic conditions (when requested) and have no historical tracking. |

**Why Snapshot for All Objects:**
- No `updated_at` or `modified_since` filter available
- No change data capture (CDC) support
- No cursor-based pagination for tracking new/changed records
- Each request returns current point-in-time data
- Distance/duration values change based on real-time traffic conditions

**Recommended Sync Strategy:**
- Perform full refresh syncs at regular intervals
- Use consistent search queries to maintain data coverage
- Consider the cost implications of frequent full syncs due to API pricing
- For geocoder: Maintain a list of addresses to geocode and refresh periodically
- For distance_matrix: Re-calculate during relevant time windows if traffic-aware data is needed

---

## **Read API for Data Retrieval**

### Primary Read Method: Text Search (New)

The Text Search (New) API returns places based on a text search string. This is the recommended method for retrieving place data.

**Endpoint:**
```
POST https://places.googleapis.com/v1/places:searchText
```

**Required Headers:**
| Header | Value | Description |
|--------|-------|-------------|
| `Content-Type` | `application/json` | Request content type |
| `X-Goog-Api-Key` | `YOUR_API_KEY` | Authentication |
| `X-Goog-FieldMask` | Field paths | Specifies which fields to return |

**Request Body Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `textQuery` | string | Yes | The text search query (e.g., "restaurants in Paris") |
| `includedType` | string | No | Restrict results to a specific place type |
| `languageCode` | string | No | Language for results (e.g., "en") |
| `locationBias` | object | No | Bias results to a specific area |
| `locationRestriction` | object | No | Restrict results to a specific area |
| `maxResultCount` | integer | No | Maximum results (1-20, default 20) |
| `minRating` | number | No | Minimum average rating filter |
| `openNow` | boolean | No | Only return places open now |
| `pageToken` | string | No | Token for next page of results |
| `priceLevels` | array | No | Filter by price levels |
| `rankPreference` | string | No | `DISTANCE` or `RELEVANCE` |
| `regionCode` | string | No | Region code for biasing (e.g., "US") |
| `strictTypeFiltering` | boolean | No | Only return exact type matches |

**Example Request:**

```bash
curl -X POST "https://places.googleapis.com/v1/places:searchText" \
  -H "Content-Type: application/json" \
  -H "X-Goog-Api-Key: YOUR_API_KEY" \
  -H "X-Goog-FieldMask: places.id,places.displayName,places.formattedAddress,places.location,places.rating,places.userRatingCount,places.types,places.businessStatus,places.websiteUri,places.internationalPhoneNumber" \
  -d '{
    "textQuery": "coffee shops in Seattle",
    "maxResultCount": 20,
    "languageCode": "en"
  }'
```

**Example Response:**

```json
{
  "places": [
    {
      "id": "ChIJcWGw3NJqkFQR3qF7WBkUkcU",
      "displayName": {
        "text": "Starbucks Reserve Roastery",
        "languageCode": "en"
      },
      "formattedAddress": "1124 Pike St, Seattle, WA 98101, USA",
      "location": {
        "latitude": 47.6142929,
        "longitude": -122.3284203
      },
      "rating": 4.6,
      "userRatingCount": 15234,
      "types": ["cafe", "coffee_shop", "food", "point_of_interest", "store", "establishment"],
      "businessStatus": "OPERATIONAL",
      "websiteUri": "https://www.starbucksreserve.com/",
      "internationalPhoneNumber": "+1 206-624-0173"
    },
    {
      "id": "ChIJ2dGMjc1qkFQRlnFMl03yYx0",
      "displayName": {
        "text": "Elm Coffee Roasters",
        "languageCode": "en"
      },
      "formattedAddress": "240 2nd Ave S, Seattle, WA 98104, USA",
      "location": {
        "latitude": 47.6003918,
        "longitude": -122.3318741
      },
      "rating": 4.7,
      "userRatingCount": 892,
      "types": ["cafe", "coffee_shop", "food", "point_of_interest", "establishment"],
      "businessStatus": "OPERATIONAL"
    }
  ],
  "nextPageToken": "AeJbb3..."
}
```

### Pagination

The Text Search API supports pagination through the `nextPageToken`:

1. Initial request returns up to 20 results and a `nextPageToken` (if more results exist)
2. Use `pageToken` in subsequent requests to fetch additional pages
3. Maximum of 60 results total (3 pages) per search query

**Pagination Example:**

```bash
# Subsequent page request
curl -X POST "https://places.googleapis.com/v1/places:searchText" \
  -H "Content-Type: application/json" \
  -H "X-Goog-Api-Key: YOUR_API_KEY" \
  -H "X-Goog-FieldMask: places.id,places.displayName,places.formattedAddress" \
  -d '{
    "textQuery": "coffee shops in Seattle",
    "pageToken": "AeJbb3..."
  }'
```

### Alternative Read Method: Nearby Search (New)

For location-based searches within a specific area:

**Endpoint:**
```
POST https://places.googleapis.com/v1/places:searchNearby
```

**Key Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `locationRestriction` | object | Yes | Circle defining search area |
| `includedTypes` | array | No | Place types to include |
| `excludedTypes` | array | No | Place types to exclude |
| `maxResultCount` | integer | No | Maximum results (1-20) |

### Place Details (for enrichment)

To get complete details for a known Place ID:

**Endpoint:**
```
GET https://places.googleapis.com/v1/places/{place_id}
```

**Example:**
```bash
curl "https://places.googleapis.com/v1/places/ChIJcWGw3NJqkFQR3qF7WBkUkcU" \
  -H "X-Goog-Api-Key: YOUR_API_KEY" \
  -H "X-Goog-FieldMask: id,displayName,formattedAddress,rating,reviews"
```

### Deleted Records (Places)

**TBD:** The Google Maps Places API does not provide an endpoint or mechanism for tracking deleted places. Places may become unavailable (return 404) but there is no change feed for deletions.

**Recommendation:** Handle missing places gracefully during sync; a previously-known Place ID returning 404 may indicate closure or removal.

---

## **Read API for Geocoder Object**

### Primary Read Method: Geocoding API

The Geocoding API converts addresses into geographic coordinates (forward geocoding) and coordinates into addresses (reverse geocoding).

**Endpoint:**
```
GET https://maps.googleapis.com/maps/api/geocode/json
```

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `address` | string | Yes* | Address to geocode (required for forward geocoding) |
| `latlng` | string | Yes* | Coordinates to reverse geocode, format: "lat,lng" (required for reverse geocoding) |
| `key` | string | Yes | API key for authentication |
| `bounds` | string | No | Bounding box to bias results, format: "lat,lng\|lat,lng" |
| `language` | string | No | Language code for results (e.g., "en", "de") |
| `region` | string | No | Region code to bias results (e.g., "us", "de") |
| `components` | string | No | Component filter, format: "component:value\|component:value" |
| `place_id` | string | No | Place ID to geocode (alternative to address) |
| `result_type` | string | No | Filter by address type (reverse geocoding only) |
| `location_type` | string | No | Filter by location type (reverse geocoding only) |

*Either `address`, `latlng`, or `place_id` is required.

**Component Filter Values:**
| Component | Description |
|-----------|-------------|
| `route` | Street name |
| `locality` | City or town |
| `administrative_area` | State or province |
| `postal_code` | ZIP or postal code |
| `country` | Country (ISO 3166-1 code) |

### Forward Geocoding Example

Convert an address to coordinates:

**Request:**
```bash
curl "https://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&key=YOUR_API_KEY"
```

**Response:**
```json
{
  "results": [
    {
      "address_components": [
        {
          "long_name": "1600",
          "short_name": "1600",
          "types": ["street_number"]
        },
        {
          "long_name": "Amphitheatre Parkway",
          "short_name": "Amphitheatre Pkwy",
          "types": ["route"]
        },
        {
          "long_name": "Mountain View",
          "short_name": "Mountain View",
          "types": ["locality", "political"]
        },
        {
          "long_name": "Santa Clara County",
          "short_name": "Santa Clara County",
          "types": ["administrative_area_level_2", "political"]
        },
        {
          "long_name": "California",
          "short_name": "CA",
          "types": ["administrative_area_level_1", "political"]
        },
        {
          "long_name": "United States",
          "short_name": "US",
          "types": ["country", "political"]
        },
        {
          "long_name": "94043",
          "short_name": "94043",
          "types": ["postal_code"]
        }
      ],
      "formatted_address": "1600 Amphitheatre Pkwy, Mountain View, CA 94043, USA",
      "geometry": {
        "location": {
          "lat": 37.4224764,
          "lng": -122.0842499
        },
        "location_type": "ROOFTOP",
        "viewport": {
          "northeast": {
            "lat": 37.4238253802915,
            "lng": -122.0829009197085
          },
          "southwest": {
            "lat": 37.4211274197085,
            "lng": -122.0855988802915
          }
        }
      },
      "place_id": "ChIJ2eUgeAK6j4ARbn5u_wAGqWA",
      "plus_code": {
        "compound_code": "CWC8+W5 Mountain View, CA, USA",
        "global_code": "849VCWC8+W5"
      },
      "types": ["street_address"]
    }
  ],
  "status": "OK"
}
```

### Reverse Geocoding Example

Convert coordinates to an address:

**Request:**
```bash
curl "https://maps.googleapis.com/maps/api/geocode/json?latlng=37.4224764,-122.0842499&key=YOUR_API_KEY"
```

**Response:**
```json
{
  "results": [
    {
      "address_components": [
        {
          "long_name": "1600",
          "short_name": "1600",
          "types": ["street_number"]
        },
        {
          "long_name": "Amphitheatre Parkway",
          "short_name": "Amphitheatre Pkwy",
          "types": ["route"]
        },
        {
          "long_name": "Mountain View",
          "short_name": "Mountain View",
          "types": ["locality", "political"]
        },
        {
          "long_name": "California",
          "short_name": "CA",
          "types": ["administrative_area_level_1", "political"]
        },
        {
          "long_name": "United States",
          "short_name": "US",
          "types": ["country", "political"]
        },
        {
          "long_name": "94043",
          "short_name": "94043",
          "types": ["postal_code"]
        }
      ],
      "formatted_address": "1600 Amphitheatre Pkwy, Mountain View, CA 94043, USA",
      "geometry": {
        "location": {
          "lat": 37.4224764,
          "lng": -122.0842499
        },
        "location_type": "ROOFTOP",
        "viewport": {
          "northeast": {
            "lat": 37.4238253802915,
            "lng": -122.0829009197085
          },
          "southwest": {
            "lat": 37.4211274197085,
            "lng": -122.0855988802915
          }
        }
      },
      "place_id": "ChIJ2eUgeAK6j4ARbn5u_wAGqWA",
      "types": ["street_address"]
    }
  ],
  "status": "OK"
}
```

### Geocoding with Place ID

Geocode a known Place ID:

**Request:**
```bash
curl "https://maps.googleapis.com/maps/api/geocode/json?place_id=ChIJ2eUgeAK6j4ARbn5u_wAGqWA&key=YOUR_API_KEY"
```

### Component Filtering Example

Restrict results to a specific country:

**Request:**
```bash
curl "https://maps.googleapis.com/maps/api/geocode/json?address=Paris&components=country:FR&key=YOUR_API_KEY"
```

### Pagination

The Geocoding API does **not** support pagination. Each request returns all matching results (typically 1-5 results ordered by relevance). For addresses with multiple interpretations, multiple results are returned in the `results` array.

### Deleted Records (Geocoder)

Not applicable. The Geocoding API is a stateless query service - it does not store or track records. Each request is independent and returns current geocoding data based on Google's address database.

---

## **Read API for Distance Matrix Object**

### Primary Read Method: Distance Matrix API

The Distance Matrix API calculates travel distance and time for a matrix of origins and destinations, based on the recommended route between each pair.

**Endpoint:**
```
GET https://maps.googleapis.com/maps/api/distancematrix/json
```

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `origins` | string | Yes | Pipe-separated list of origins (addresses, coordinates, or place IDs) |
| `destinations` | string | Yes | Pipe-separated list of destinations (addresses, coordinates, or place IDs) |
| `key` | string | Yes | API key for authentication |
| `mode` | string | No | Travel mode: `driving` (default), `walking`, `bicycling`, `transit` |
| `language` | string | No | Language code for results (e.g., "en", "de") |
| `region` | string | No | Region code to bias results (e.g., "us", "de") |
| `avoid` | string | No | Features to avoid: `tolls`, `highways`, `ferries`, `indoor` (pipe-separated) |
| `units` | string | No | Unit system: `metric` (default) or `imperial` |
| `arrival_time` | integer | No | Desired arrival time as Unix timestamp (transit mode only) |
| `departure_time` | integer | No | Departure time as Unix timestamp or `now` |
| `traffic_model` | string | No | Traffic prediction model: `best_guess`, `pessimistic`, `optimistic` |
| `transit_mode` | string | No | Transit modes: `bus`, `subway`, `train`, `tram`, `rail` (pipe-separated) |
| `transit_routing_preference` | string | No | Transit preference: `less_walking`, `fewer_transfers` |

**Origin/Destination Formats:**
- Address: `"1600 Amphitheatre Parkway, Mountain View, CA"`
- Coordinates: `"37.4224764,-122.0842499"`
- Place ID: `"place_id:ChIJ2eUgeAK6j4ARbn5u_wAGqWA"`

### Basic Distance Matrix Example

Calculate distances between two origins and two destinations:

**Request:**
```bash
curl "https://maps.googleapis.com/maps/api/distancematrix/json?origins=Seattle,WA|San+Francisco,CA&destinations=Los+Angeles,CA|Portland,OR&key=YOUR_API_KEY"
```

**Response:**
```json
{
  "status": "OK",
  "origin_addresses": [
    "Seattle, WA, USA",
    "San Francisco, CA, USA"
  ],
  "destination_addresses": [
    "Los Angeles, CA, USA",
    "Portland, OR, USA"
  ],
  "rows": [
    {
      "elements": [
        {
          "status": "OK",
          "distance": {
            "value": 1827354,
            "text": "1,827 km"
          },
          "duration": {
            "value": 62340,
            "text": "17 hours 19 mins"
          }
        },
        {
          "status": "OK",
          "distance": {
            "value": 279201,
            "text": "279 km"
          },
          "duration": {
            "value": 10080,
            "text": "2 hours 48 mins"
          }
        }
      ]
    },
    {
      "elements": [
        {
          "status": "OK",
          "distance": {
            "value": 616555,
            "text": "617 km"
          },
          "duration": {
            "value": 22680,
            "text": "6 hours 18 mins"
          }
        },
        {
          "status": "OK",
          "distance": {
            "value": 1010342,
            "text": "1,010 km"
          },
          "duration": {
            "value": 36180,
            "text": "10 hours 3 mins"
          }
        }
      ]
    }
  ]
}
```

### Distance Matrix with Traffic Example

Get driving times with real-time traffic:

**Request:**
```bash
curl "https://maps.googleapis.com/maps/api/distancematrix/json?origins=Seattle,WA&destinations=Portland,OR&mode=driving&departure_time=now&traffic_model=best_guess&key=YOUR_API_KEY"
```

**Response:**
```json
{
  "status": "OK",
  "origin_addresses": ["Seattle, WA, USA"],
  "destination_addresses": ["Portland, OR, USA"],
  "rows": [
    {
      "elements": [
        {
          "status": "OK",
          "distance": {
            "value": 279201,
            "text": "279 km"
          },
          "duration": {
            "value": 10080,
            "text": "2 hours 48 mins"
          },
          "duration_in_traffic": {
            "value": 11520,
            "text": "3 hours 12 mins"
          }
        }
      ]
    }
  ]
}
```

### Transit Mode Example

Get public transit times with fare information:

**Request:**
```bash
curl "https://maps.googleapis.com/maps/api/distancematrix/json?origins=Penn+Station,New+York,NY&destinations=Grand+Central,New+York,NY&mode=transit&departure_time=now&key=YOUR_API_KEY"
```

**Response (transit with fare):**
```json
{
  "status": "OK",
  "origin_addresses": ["Penn Station, New York, NY, USA"],
  "destination_addresses": ["Grand Central Terminal, New York, NY, USA"],
  "rows": [
    {
      "elements": [
        {
          "status": "OK",
          "distance": {
            "value": 1200,
            "text": "1.2 km"
          },
          "duration": {
            "value": 600,
            "text": "10 mins"
          },
          "fare": {
            "currency": "USD",
            "value": 2.90,
            "text": "$2.90"
          }
        }
      ]
    }
  ]
}
```

### Using Place IDs

Request using Place IDs instead of addresses:

**Request:**
```bash
curl "https://maps.googleapis.com/maps/api/distancematrix/json?origins=place_id:ChIJ2eUgeAK6j4ARbn5u_wAGqWA&destinations=place_id:ChIJIQBpAG2ahYAR_6128GcTUEo&key=YOUR_API_KEY"
```

### Pagination

The Distance Matrix API does **not** support pagination. Results are limited by:
- Maximum 25 origins per request
- Maximum 25 destinations per request
- Maximum 100 elements (origins × destinations) per request for free tier
- Up to 625 elements per request (25×25) for higher tiers

For larger matrices, batch requests into multiple API calls.

### Deleted Records (Distance Matrix)

Not applicable. The Distance Matrix API is a stateless query service - it does not store or track records. Each request is independent and calculates distances/durations based on current road network and traffic conditions.

---

### Rate Limits

**Places API:**
| Limit Type | Value | Notes |
|------------|-------|-------|
| Requests per second (QPS) | No hard limit published | Subject to quota |
| Daily quota | Based on billing account | Check Cloud Console for limits |
| Results per request | 20 max | Use pagination for more |
| Max results per query | 60 | 3 pages maximum |

**Geocoding API:**
| Limit Type | Value | Notes |
|------------|-------|-------|
| Requests per second (QPS) | 50 QPS | Default limit per project |
| Daily quota | Based on billing account | Check Cloud Console for limits |
| Results per request | 1-5 typical | No pagination, returns all matches |

**Distance Matrix API:**
| Limit Type | Value | Notes |
|------------|-------|-------|
| Requests per second (QPS) | 1000 elements/sec | Elements = origins × destinations |
| Max origins per request | 25 | Hard limit |
| Max destinations per request | 25 | Hard limit |
| Max elements per request | 100 (free) / 625 (paid) | origins × destinations |
| Daily quota | Based on billing account | Check Cloud Console for limits |

**Cost Considerations:**
- All APIs are paid services
- Places API pricing varies by SKU (Text Search, Place Details, etc.)
- Field masks affect Places API pricing - basic fields are cheaper than contact/atmosphere data
- Geocoding API charges per request
- Distance Matrix API charges per element (each origin-destination pair)
- Check [Google Maps Platform Pricing](https://developers.google.com/maps/documentation/places/web-service/usage-and-billing) for current rates

---

## **Field Type Mapping**

| API Field Type | Standard Type | Notes |
|----------------|---------------|-------|
| `string` | STRING | Direct mapping |
| `number` | DOUBLE | Used for lat/lng, ratings |
| `integer` | INTEGER | Used for counts |
| `boolean` | BOOLEAN | Direct mapping |
| `object` | STRUCT | Nested objects (displayName, location, geometry) |
| `array` | ARRAY | Lists of values or objects |
| `enum` (string) | STRING | businessStatus, priceLevel, location_type |

### Special Field Behaviors (Places)

| Field | Behavior |
|-------|----------|
| `id` | Auto-generated by Google, immutable, alphanumeric string |
| `rating` | Computed average, range 1.0-5.0, may be absent if insufficient reviews |
| `userRatingCount` | Computed count, may be absent |
| `businessStatus` | Enum: `OPERATIONAL`, `CLOSED_TEMPORARILY`, `CLOSED_PERMANENTLY` |
| `priceLevel` | Enum with 5 levels from FREE to VERY_EXPENSIVE |
| `types` | Array of predefined place type strings |
| `location` | Nested object with latitude/longitude as numbers |
| `displayName` | Nested object with text and languageCode |
| `photos` | Array of photo references (requires separate Photo API call to get actual images) |
| `reviews` | Array of review objects with author, rating, text, time |

### Special Field Behaviors (Geocoder)

| Field | Behavior |
|-------|----------|
| `place_id` | Auto-generated by Google, can be used with Places API |
| `formatted_address` | Full human-readable address string |
| `geometry.location_type` | Enum: `ROOFTOP`, `RANGE_INTERPOLATED`, `GEOMETRIC_CENTER`, `APPROXIMATE` |
| `partial_match` | Boolean, true if geocoder did not find exact match for input |
| `address_components` | Array of components, each with long_name, short_name, and types |
| `geometry.location` | Nested object with lat/lng as numbers |
| `geometry.viewport` | Nested object with northeast/southwest bounds |
| `geometry.bounds` | Optional, present for results covering an area (e.g., cities) |
| `plus_code` | Optional Plus Code with global_code and compound_code |
| `types` | Array of address type strings indicating what the result represents |

### Special Field Behaviors (Distance Matrix)

| Field | Behavior |
|-------|----------|
| `status` | Top-level and element-level status codes (see status enums above) |
| `origin_addresses` | Array of interpreted addresses, same order as request origins |
| `destination_addresses` | Array of interpreted addresses, same order as request destinations |
| `distance.value` | Distance in meters (integer) |
| `distance.text` | Human-readable distance, respects `units` parameter |
| `duration.value` | Duration in seconds (integer), without traffic |
| `duration.text` | Human-readable duration (e.g., "2 hours 48 mins") |
| `duration_in_traffic` | Only present when `departure_time` is specified and mode is `driving` |
| `fare` | Only present for `transit` mode in supported regions |
| `rows` | Array index corresponds to origin index |
| `elements` | Array index corresponds to destination index |

### Place Types

Place types are predefined strings. Common examples:
- `restaurant`, `cafe`, `bar`
- `lodging`, `hotel`
- `store`, `shopping_mall`
- `hospital`, `pharmacy`
- `park`, `museum`, `tourist_attraction`
- `airport`, `train_station`

See [Place Types (New)](https://developers.google.com/maps/documentation/places/web-service/place-types) for the complete list.

---

## **Sources and References**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://developers.google.com/maps/documentation/places/web-service | 2026-01-06 | Highest | Places API overview, endpoints, features |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/text-search | 2026-01-06 | Highest | Text Search endpoint, parameters, response format |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/place-details | 2026-01-06 | Highest | Place Details endpoint |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/data-fields | 2026-01-06 | Highest | Complete field list and schema |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/place-types | 2026-01-06 | Highest | Place types enumeration |
| Official Docs | https://developers.google.com/maps/documentation/javascript/reference/geocoder | 2026-01-08 | Highest | Geocoder class, interfaces, response format, status codes |
| Official Docs | https://developers.google.com/maps/documentation/geocoding | 2026-01-08 | Highest | Geocoding API web service endpoint, parameters |
| Official Docs | https://developers.google.com/maps/documentation/javascript/reference/distance-matrix | 2026-01-08 | Highest | Distance Matrix service, interfaces, response format, status codes |
| Official Docs | https://developers.google.com/maps/documentation/distance-matrix | 2026-01-08 | Highest | Distance Matrix API web service endpoint, parameters |

**Notes:**
- All documentation is from the official Google Maps Platform documentation
- The Places API (New) is the current recommended version for places data
- The Geocoding API is available as both a web service and JavaScript library; this connector uses the web service
- The Distance Matrix API is available as both a web service and JavaScript library; this connector uses the web service
- Schema and field information based on official documentation
- No conflicting sources; single authoritative source used

