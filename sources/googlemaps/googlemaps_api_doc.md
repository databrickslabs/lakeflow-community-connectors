# **Google Maps Places API Documentation**

## **Authorization**

### Preferred Method: API Key

Google Maps Places API uses API Key authentication. The API key is passed as a query parameter or header with each request.

**Authentication Parameters:**
| Parameter | Location | Required | Description |
|-----------|----------|----------|-------------|
| `key` | Query parameter | Yes | Your Google Maps Platform API key |

**Alternative:** The API key can also be passed in the `X-Goog-Api-Key` header.

**How to obtain an API key:**
1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the "Places API" or "Places API (New)" from the API Library
4. Navigate to "Credentials" and create an API key
5. (Recommended) Restrict the API key to only the Places API

**Example API Request with Authentication:**

```bash
# Using query parameter
curl "https://places.googleapis.com/v1/places:searchText" \
  -H "Content-Type: application/json" \
  -H "X-Goog-Api-Key: YOUR_API_KEY" \
  -H "X-Goog-FieldMask: places.id,places.displayName,places.formattedAddress" \
  -d '{
    "textQuery": "restaurants in New York"
  }'
```

**Important Notes:**
- API keys should be kept secure and not exposed in client-side code
- Enable billing on your Google Cloud project (required for Places API)
- Set up API key restrictions to prevent unauthorized usage

---

## **Object List**

The object list for this connector is **static**. This connector supports a single object:

| Object Name | Description | API Endpoint |
|-------------|-------------|--------------|
| `places` | Location data including businesses, points of interest, and geographic locations | Text Search (New) API |

**Object Description:**

The `places` object represents location data from Google's database of over 200 million places. Each place record includes:
- Unique place identifier
- Name and address information
- Geographic coordinates
- Business information (hours, ratings, reviews)
- Contact information
- Photos and other rich data

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

## **Get Object Primary Keys**

The primary key for the `places` object is **static** and does not require an API call.

| Object | Primary Key Column | Type | Description |
|--------|-------------------|------|-------------|
| `places` | `id` | string | The Place ID - a unique identifier that represents a specific place in the Google Places database |

**Place ID Characteristics:**
- Globally unique identifier
- Stable over time (though may occasionally change)
- Can be used to retrieve place details directly
- Format: Alphanumeric string (e.g., `ChIJN1t_tDeuEmsRUsoyG83frY4`)

---

## **Object's Ingestion Type**

| Object | Ingestion Type | Rationale |
|--------|---------------|-----------|
| `places` | `snapshot` | The Places API does not provide change feeds, cursors, or timestamps for incremental sync. Each query returns the current state of matching places. Place data can change (ratings, hours, etc.) but there's no mechanism to track changes over time. |

**Why Snapshot:**
- No `updated_at` or `modified_since` filter available
- No change data capture (CDC) support
- No cursor-based pagination for tracking new/changed records
- Each search returns current point-in-time data

**Recommended Sync Strategy:**
- Perform full refresh syncs at regular intervals
- Use consistent search queries to maintain data coverage
- Consider the cost implications of frequent full syncs due to API pricing

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

### Deleted Records

**TBD:** The Google Maps Places API does not provide an endpoint or mechanism for tracking deleted places. Places may become unavailable (return 404) but there is no change feed for deletions.

**Recommendation:** Handle missing places gracefully during sync; a previously-known Place ID returning 404 may indicate closure or removal.

### Rate Limits

| Limit Type | Value | Notes |
|------------|-------|-------|
| Requests per second (QPS) | No hard limit published | Subject to quota |
| Daily quota | Based on billing account | Check Cloud Console for limits |
| Results per request | 20 max | Use pagination for more |
| Max results per query | 60 | 3 pages maximum |

**Cost Considerations:**
- Places API is a paid service
- Pricing varies by SKU (Text Search, Place Details, etc.)
- Field masks affect pricing - basic fields are cheaper than contact/atmosphere data
- Check [Google Maps Platform Pricing](https://developers.google.com/maps/documentation/places/web-service/usage-and-billing) for current rates

---

## **Field Type Mapping**

| API Field Type | Standard Type | Notes |
|----------------|---------------|-------|
| `string` | STRING | Direct mapping |
| `number` | DOUBLE | Used for lat/lng, ratings |
| `integer` | INTEGER | Used for counts |
| `boolean` | BOOLEAN | Direct mapping |
| `object` | STRUCT | Nested objects (displayName, location) |
| `array` | ARRAY | Lists of values or objects |
| `enum` (string) | STRING | businessStatus, priceLevel |

### Special Field Behaviors

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
| Official Docs | https://developers.google.com/maps/documentation/places/web-service | 2026-01-06 | Highest | API overview, endpoints, features |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/text-search | 2026-01-06 | Highest | Text Search endpoint, parameters, response format |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/place-details | 2026-01-06 | Highest | Place Details endpoint |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/data-fields | 2026-01-06 | Highest | Complete field list and schema |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/place-types | 2026-01-06 | Highest | Place types enumeration |

**Notes:**
- All documentation is from the official Google Maps Platform documentation
- The Places API (New) is the current recommended version
- Schema and field information based on official Place Data Fields documentation
- No conflicting sources; single authoritative source used

