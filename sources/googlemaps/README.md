# Lakeflow Google Maps Community Connector

This documentation provides setup instructions and reference information for the Google Maps Places source connector. This connector uses the Google Places API (New) to retrieve location data including businesses, points of interest, and geographic locations.

## Prerequisites

- **Google Cloud Project**: An active Google Cloud project with billing enabled
- **Places API Enabled**: The "Places API (New)" must be enabled in your Google Cloud project
- **API Key**: A Google Maps Platform API key with Places API permissions

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `api_key` | string | Yes | Google Maps Platform API key | `AIzaSyD...` |
| `externalOptionsAllowList` | string | Yes | Comma-separated list of table-specific option names allowed to be passed to the connector | `text_query,language_code,max_result_count,included_type,min_rating,open_now,region_code` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`text_query,language_code,max_result_count,included_type,min_rating,open_now,region_code`

> **Note**: Table-specific options like `text_query` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining Your API Key

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable billing for your project (required for Places API)
4. Navigate to **APIs & Services** → **Library**
5. Search for and enable "Places API (New)"
6. Navigate to **APIs & Services** → **Credentials**
7. Click **Create Credentials** → **API Key**
8. (Recommended) Restrict the API key:
   - Click on the newly created key
   - Under "API restrictions", select "Restrict key"
   - Select only "Places API (New)"

**Important**: Keep your API key secure and never expose it in client-side code.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one
3. Set `externalOptionsAllowList` to `text_query,language_code,max_result_count,included_type,min_rating,open_now,region_code` (required for this connector to pass table-specific options)

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Google Maps connector supports a **single static object**:

| Table Name | Description | Primary Key | Ingestion Type |
|------------|-------------|-------------|----------------|
| `places` | Location data from Google's database of over 200 million places | `id` | `snapshot` |

### Object Details

#### `places`

The `places` object retrieves location data using the Text Search API. Each place record includes:

- Unique place identifier
- Name and address information
- Geographic coordinates (latitude/longitude)
- Business information (hours, ratings, reviews count)
- Contact information (phone, website)
- Service attributes (takeout, delivery, dine-in, etc.)
- Accessibility and parking information

**Ingestion Type**: Snapshot only. The Places API does not support incremental synchronization—each query returns the current state of matching places.

**Result Limit**: The API returns a maximum of 60 results per search query (3 pages of 20 results).

### Required and Optional Table Options

Table options are provided per-table in the pipeline specification:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `text_query` | string | Yes | The search query for places | `"restaurants in Seattle"` |
| `language_code` | string | No | Language code for results | `"en"` |
| `max_result_count` | string | No | Maximum results per page (1-20, default 20) | `"20"` |
| `included_type` | string | No | Restrict to a specific place type | `"restaurant"` |
| `min_rating` | string | No | Minimum average rating filter (1.0-5.0) | `"4.0"` |
| `open_now` | string | No | Only return places currently open | `"true"` |
| `region_code` | string | No | Region code for biasing results | `"US"` |

### Schema Highlights

The places schema includes both simple fields and nested structures:

**Core Fields:**
- `id`: Unique Google Place ID (primary key)
- `formattedAddress`: Human-readable address
- `rating`: Average user rating (1.0 to 5.0)
- `userRatingCount`: Total number of user ratings
- `businessStatus`: `OPERATIONAL`, `CLOSED_TEMPORARILY`, or `CLOSED_PERMANENTLY`
- `priceLevel`: Price indicator from `PRICE_LEVEL_FREE` to `PRICE_LEVEL_VERY_EXPENSIVE`

**Nested Structures:**
- `displayName`: Contains `text` (place name) and `languageCode`
- `location`: Contains `latitude` and `longitude`
- `addressComponents`: Array of address components with types
- `currentOpeningHours` / `regularOpeningHours`: Opening hours with periods and descriptions
- `accessibilityOptions`: Wheelchair accessibility features
- `parkingOptions`: Parking availability information
- `paymentOptions`: Accepted payment methods

**Service Attributes (boolean):**
- `takeout`, `delivery`, `dineIn`, `reservable`
- `servesBreakfast`, `servesLunch`, `servesDinner`, `servesBrunch`
- `servesBeer`, `servesWine`, `servesVegetarianFood`
- `outdoorSeating`, `liveMusic`, `goodForGroups`, `allowsDogs`

## Data Type Mapping

| Google Places Type | Spark Type | Notes |
|--------------------|------------|-------|
| string | StringType | Direct mapping |
| number (lat/lng, rating) | DoubleType | Floating-point values |
| integer (counts) | LongType | Numeric counts |
| boolean | BooleanType | Service flags like `takeout`, `delivery` |
| object | StructType | Nested structures like `displayName`, `location` |
| array | ArrayType | Lists such as `types`, `addressComponents` |
| enum (string) | StringType | `businessStatus`, `priceLevel` |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`)
2. Each table entry **must** include a `text_query` option specifying what places to search for

Example `pipeline_spec`:

```json
{
  "pipeline_spec": {
    "connection_name": "googlemaps_connection",
    "object": [
      {
        "table": {
          "source_table": "places",
          "text_query": "coffee shops in Seattle",
          "language_code": "en",
          "min_rating": "4.0"
        }
      },
      {
        "table": {
          "source_table": "places",
          "text_query": "restaurants in San Francisco",
          "included_type": "restaurant",
          "open_now": "true"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your Google Maps `api_key`
- Each `table` entry requires `source_table: "places"` and a `text_query`
- You can have multiple table entries with different search queries to ingest various location datasets

3. (Optional) Customize the source connector code if needed for special use cases

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin with a focused search query (e.g., specific city and category) to validate configuration
- **Use Specific Queries**: More specific text queries yield more relevant results
- **Consider Costs**: The Places API is a paid service—balance data freshness with API usage
- **Apply Filters**: Use `included_type`, `min_rating`, or `open_now` to narrow results and reduce unnecessary API calls

#### Scheduling Considerations

Since the connector uses snapshot ingestion:
- Each run performs a complete refresh of the data
- Schedule syncs based on how frequently place data changes for your use case
- Consider staggering syncs across different search queries to spread API costs

## Troubleshooting

**Common Issues:**

**Authentication Errors (401/403):**
- Verify that the `api_key` is correct and not expired
- Ensure the Places API (New) is enabled in your Google Cloud project
- Check that billing is enabled for your project

**No Results Returned:**
- Verify the `text_query` is specific enough and matches existing places
- Try broadening the search query or removing restrictive filters
- Check if `included_type` matches valid Google place types

**Missing Required Parameter Error:**
- Ensure `text_query` is provided for every `places` table entry in your pipeline spec
- Verify `externalOptionsAllowList` includes `text_query` in your connection configuration

**Rate Limiting:**
- The Places API has usage quotas—check your Google Cloud Console for current limits
- Reduce sync frequency or spread queries across multiple pipeline runs

**Unexpected Schema/Data:**
- Nested objects (like `displayName`, `location`) require appropriate handling downstream
- Some fields may be `null` if the place doesn't have that information

## References

- **Connector Implementation**: `sources/googlemaps/googlemaps.py`
- **API Documentation**: `sources/googlemaps/googlemaps_api_doc.md`
- **Official Google Documentation**:
  - [Places API Overview](https://developers.google.com/maps/documentation/places/web-service)
  - [Text Search (New)](https://developers.google.com/maps/documentation/places/web-service/text-search)
  - [Place Data Fields](https://developers.google.com/maps/documentation/places/web-service/data-fields)
  - [Place Types](https://developers.google.com/maps/documentation/places/web-service/place-types)
  - [Usage and Billing](https://developers.google.com/maps/documentation/places/web-service/usage-and-billing)

