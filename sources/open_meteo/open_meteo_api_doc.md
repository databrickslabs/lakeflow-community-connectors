# **Open-Meteo API Documentation**

## **Authorization**

Open-Meteo uses **no authentication** for non-commercial use. The API is freely accessible without any API key, registration, or credentials.

- **Method**: None (public API)
- **Headers**: No authorization headers required
- **Rate Limits**: Fair use policy; contact Open-Meteo if exceeding 10,000 requests per day
- **Commercial Use**: For commercial applications, contact info@open-meteo.com for API subscription

**Example Request (no auth required):**
```bash
curl "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m"
```

---

## **Object List**

The object list is **static**. Open-Meteo provides weather data organized by API endpoint, which we model as tables:

| Table Name | Description | API Endpoint |
|------------|-------------|--------------|
| `forecast` | Hourly weather forecast for up to 16 days | `/v1/forecast` |
| `historical` | Historical weather data (80+ years, ERA5 reanalysis) | `/v1/archive` |
| `geocoding` | Location search by name to get coordinates | `/v1/search` (geocoding-api.open-meteo.com) |

**Note**: Each table requires location parameters (`latitude`, `longitude`) to retrieve data. The `geocoding` table can be used to convert city names to coordinates.

---

## **Object Schema**

The schema is **static** and defined by the available weather variables. The API returns JSON with time-series arrays.

### **Forecast Table Schema**

| Field Name | Type | Nullable | Description |
|------------|------|----------|-------------|
| `time` | STRING | No | ISO8601 timestamp (e.g., "2024-01-01T00:00") |
| `latitude` | DOUBLE | No | Latitude of the location |
| `longitude` | DOUBLE | No | Longitude of the location |
| `elevation` | DOUBLE | Yes | Elevation in meters |
| `timezone` | STRING | Yes | Timezone name (e.g., "Europe/Berlin") |
| `temperature_2m` | DOUBLE | Yes | Temperature at 2m above ground (°C) |
| `relative_humidity_2m` | DOUBLE | Yes | Relative humidity at 2m (%) |
| `dewpoint_2m` | DOUBLE | Yes | Dew point at 2m (°C) |
| `apparent_temperature` | DOUBLE | Yes | Feels-like temperature (°C) |
| `precipitation` | DOUBLE | Yes | Total precipitation (mm) |
| `rain` | DOUBLE | Yes | Rain amount (mm) |
| `showers` | DOUBLE | Yes | Showers amount (mm) |
| `snowfall` | DOUBLE | Yes | Snowfall amount (cm) |
| `snow_depth` | DOUBLE | Yes | Snow depth (m) |
| `weather_code` | LONG | Yes | WMO weather code |
| `cloud_cover` | DOUBLE | Yes | Total cloud cover (%) |
| `cloud_cover_low` | DOUBLE | Yes | Low-level cloud cover (%) |
| `cloud_cover_mid` | DOUBLE | Yes | Mid-level cloud cover (%) |
| `cloud_cover_high` | DOUBLE | Yes | High-level cloud cover (%) |
| `pressure_msl` | DOUBLE | Yes | Mean sea level pressure (hPa) |
| `surface_pressure` | DOUBLE | Yes | Surface pressure (hPa) |
| `wind_speed_10m` | DOUBLE | Yes | Wind speed at 10m (km/h) |
| `wind_direction_10m` | DOUBLE | Yes | Wind direction at 10m (°) |
| `wind_gusts_10m` | DOUBLE | Yes | Wind gusts at 10m (km/h) |
| `visibility` | DOUBLE | Yes | Visibility (m) |
| `uv_index` | DOUBLE | Yes | UV index |
| `uv_index_clear_sky` | DOUBLE | Yes | UV index under clear sky |
| `is_day` | LONG | Yes | 1 if day, 0 if night |
| `sunshine_duration` | DOUBLE | Yes | Sunshine duration (seconds) |
| `shortwave_radiation` | DOUBLE | Yes | Shortwave solar radiation (W/m²) |
| `direct_radiation` | DOUBLE | Yes | Direct solar radiation (W/m²) |
| `diffuse_radiation` | DOUBLE | Yes | Diffuse solar radiation (W/m²) |
| `et0_fao_evapotranspiration` | DOUBLE | Yes | Reference evapotranspiration (mm) |

### **Historical Table Schema**

Same fields as the Forecast table, with data available from 1940 onwards.

### **Geocoding Table Schema**

| Field Name | Type | Nullable | Description |
|------------|------|----------|-------------|
| `id` | LONG | No | Unique location ID |
| `name` | STRING | No | Location name |
| `latitude` | DOUBLE | No | Latitude |
| `longitude` | DOUBLE | No | Longitude |
| `elevation` | DOUBLE | Yes | Elevation in meters |
| `feature_code` | STRING | Yes | GeoNames feature code |
| `country_code` | STRING | Yes | ISO 3166-1 alpha-2 country code |
| `country` | STRING | Yes | Country name |
| `admin1` | STRING | Yes | First administrative level (state/province) |
| `admin2` | STRING | Yes | Second administrative level (county) |
| `admin3` | STRING | Yes | Third administrative level |
| `admin4` | STRING | Yes | Fourth administrative level |
| `timezone` | STRING | Yes | Timezone name |
| `population` | LONG | Yes | Population count |
| `postcodes` | ARRAY<STRING> | Yes | List of postal codes |

**Example Response (Forecast):**
```json
{
  "latitude": 52.52,
  "longitude": 13.41,
  "generationtime_ms": 0.123,
  "utc_offset_seconds": 3600,
  "timezone": "Europe/Berlin",
  "timezone_abbreviation": "CET",
  "elevation": 38.0,
  "hourly_units": {
    "time": "iso8601",
    "temperature_2m": "°C",
    "precipitation": "mm"
  },
  "hourly": {
    "time": ["2024-01-01T00:00", "2024-01-01T01:00", "..."],
    "temperature_2m": [2.5, 2.3, "..."],
    "precipitation": [0.0, 0.1, "..."]
  }
}
```

---

## **Get Object Primary Keys**

Primary keys are **static** and defined per table:

| Table | Primary Keys | Description |
|-------|--------------|-------------|
| `forecast` | `latitude`, `longitude`, `time` | Each hourly observation is unique per location and time |
| `historical` | `latitude`, `longitude`, `time` | Each hourly observation is unique per location and time |
| `geocoding` | `id` | Unique location identifier |

---

## **Object's Ingestion Type**

| Table | Ingestion Type | Cursor Field | Description |
|-------|----------------|--------------|-------------|
| `forecast` | `snapshot` | N/A | Forecasts are regenerated hourly; must re-read entire forecast window |
| `historical` | `append` | `time` | Historical data is immutable; can read incrementally by date range |
| `geocoding` | `snapshot` | N/A | Location data is relatively static; full refresh recommended |

**Rationale:**
- **Forecast**: Weather forecasts are updated hourly with new predictions. Each run should fetch the current forecast window (up to 16 days ahead). Old forecasts become stale.
- **Historical**: Historical weather data (ERA5 reanalysis) is immutable once published. New data is appended as time progresses. Can use `start_date`/`end_date` for incremental loading.
- **Geocoding**: Location database changes infrequently. Full refresh is appropriate.

---

## **Read API for Data Retrieval**

### **Forecast API**

**Endpoint:** `GET https://api.open-meteo.com/v1/forecast`

**Required Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `latitude` | float | Latitude in decimal degrees (-90 to 90) |
| `longitude` | float | Longitude in decimal degrees (-180 to 180) |

**Optional Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `hourly` | string | - | Comma-separated list of hourly variables |
| `daily` | string | - | Comma-separated list of daily variables |
| `current` | string | - | Comma-separated list of current weather variables |
| `temperature_unit` | string | `celsius` | `celsius` or `fahrenheit` |
| `wind_speed_unit` | string | `kmh` | `kmh`, `ms`, `mph`, `kn` |
| `precipitation_unit` | string | `mm` | `mm` or `inch` |
| `timeformat` | string | `iso8601` | `iso8601` or `unixtime` |
| `timezone` | string | `GMT` | Timezone name (e.g., `America/New_York`) |
| `forecast_days` | int | 7 | Number of forecast days (1-16) |
| `past_days` | int | 0 | Include past days (0-92) |
| `start_date` | string | - | Start date (YYYY-MM-DD) |
| `end_date` | string | - | End date (YYYY-MM-DD) |

**Example Request:**
```bash
curl "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m,precipitation,weather_code&forecast_days=7&timezone=Europe/Berlin"
```

**Example Response:**
```json
{
  "latitude": 52.52,
  "longitude": 13.419998,
  "generationtime_ms": 0.5960464477539062,
  "utc_offset_seconds": 3600,
  "timezone": "Europe/Berlin",
  "timezone_abbreviation": "CET",
  "elevation": 38.0,
  "hourly_units": {
    "time": "iso8601",
    "temperature_2m": "°C",
    "precipitation": "mm",
    "weather_code": "wmo code"
  },
  "hourly": {
    "time": [
      "2024-01-01T00:00",
      "2024-01-01T01:00",
      "2024-01-01T02:00"
    ],
    "temperature_2m": [2.5, 2.3, 2.1],
    "precipitation": [0.0, 0.0, 0.1],
    "weather_code": [3, 3, 61]
  }
}
```

### **Historical Weather API**

**Endpoint:** `GET https://archive-api.open-meteo.com/v1/archive`

**Required Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `latitude` | float | Latitude in decimal degrees |
| `longitude` | float | Longitude in decimal degrees |
| `start_date` | string | Start date (YYYY-MM-DD) |
| `end_date` | string | End date (YYYY-MM-DD) |

**Optional Parameters:** Same as Forecast API (`hourly`, `daily`, units, etc.)

**Example Request:**
```bash
curl "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2023-01-01&end_date=2023-01-31&hourly=temperature_2m,precipitation"
```

### **Geocoding API**

**Endpoint:** `GET https://geocoding-api.open-meteo.com/v1/search`

**Required Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | string | Location name to search |

**Optional Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `count` | int | 10 | Number of results (1-100) |
| `language` | string | `en` | Language for results |
| `format` | string | `json` | Response format |

**Example Request:**
```bash
curl "https://geocoding-api.open-meteo.com/v1/search?name=Berlin&count=5"
```

**Example Response:**
```json
{
  "results": [
    {
      "id": 2950159,
      "name": "Berlin",
      "latitude": 52.52437,
      "longitude": 13.41053,
      "elevation": 74.0,
      "feature_code": "PPLC",
      "country_code": "DE",
      "country": "Germany",
      "admin1": "Berlin",
      "timezone": "Europe/Berlin",
      "population": 3426354
    }
  ]
}
```

### **Pagination**

Open-Meteo does **not use pagination** in the traditional sense. Instead:
- **Forecast**: Returns all hourly data for the requested `forecast_days` in a single response
- **Historical**: Use `start_date` and `end_date` to control the date range (max ~1 year per request recommended)
- **Geocoding**: Use `count` parameter to limit results

### **Rate Limits**

- **Non-commercial**: Fair use policy, no strict limits
- **Recommended**: Stay under 10,000 requests per day
- **Commercial**: Contact Open-Meteo for higher limits
- **Best Practice**: Batch multiple locations if needed; cache results appropriately

### **Deleted Records**

Open-Meteo does not track deleted records. Weather data is either:
- **Forecast**: Replaced with new predictions each hour
- **Historical**: Immutable once published (no deletions)
- **Geocoding**: Location database rarely changes

---

## **Field Type Mapping**

| Open-Meteo Type | Spark Type | Notes |
|-----------------|------------|-------|
| float | DoubleType | All numeric weather values |
| integer | LongType | Weather codes, counts |
| string (ISO8601) | StringType | Timestamps (can be parsed to TimestampType) |
| string | StringType | Names, codes, timezones |
| array | ArrayType | Time series arrays, postcodes |

### **Weather Code Reference (WMO)**

| Code | Description |
|------|-------------|
| 0 | Clear sky |
| 1, 2, 3 | Mainly clear, partly cloudy, overcast |
| 45, 48 | Fog and depositing rime fog |
| 51, 53, 55 | Drizzle: Light, moderate, dense |
| 56, 57 | Freezing drizzle: Light, dense |
| 61, 63, 65 | Rain: Slight, moderate, heavy |
| 66, 67 | Freezing rain: Light, heavy |
| 71, 73, 75 | Snow fall: Slight, moderate, heavy |
| 77 | Snow grains |
| 80, 81, 82 | Rain showers: Slight, moderate, violent |
| 85, 86 | Snow showers: Slight, heavy |
| 95 | Thunderstorm: Slight or moderate |
| 96, 99 | Thunderstorm with slight/heavy hail |

---

## **Write API**

Open-Meteo is a **read-only API**. There is no write functionality available. Users cannot:
- Insert new weather data
- Update existing records
- Delete records

For write-back testing purposes, this connector will **not support write operations**.

---

## **Known Quirks & Edge Cases**

1. **Response Structure**: The API returns time-series data as parallel arrays (one array for `time`, one for each variable). The connector must "pivot" this into row-based records.

2. **Coordinate Precision**: The API may return slightly different coordinates than requested due to grid cell alignment.

3. **Missing Data**: Some variables may have `null` values for certain hours (e.g., UV index at night).

4. **Timezone Handling**: Always specify `timezone` parameter to get local times; otherwise, data is in GMT.

5. **Historical Data Lag**: ERA5 historical data has a ~5 day delay from the present.

6. **Multiple Locations**: The API supports only one location per request. For multiple locations, make separate requests.

---

## **Sources and References**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://open-meteo.com/en/docs | 2024-12-29 | High | Endpoint structure, parameters, variables |
| Official Docs | https://open-meteo.com/ | 2024-12-29 | High | Rate limits, licensing, features |
| Official Docs | https://open-meteo.com/en/docs/historical-forecast-api | 2024-12-29 | High | Historical API details |
| GitHub | https://github.com/open-meteo/open-meteo | 2024-12-29 | High | Open-source codebase, client SDKs |
| Community SDK | https://github.com/m0rp43us/openmeteopy | 2024-12-29 | Medium | Python implementation patterns |

### **Licensing**
- **API Code**: AGPLv3 license
- **Data**: Attribution 4.0 International (CC BY 4.0) - free to share and adapt with attribution

---

## **Research Log**

| Date (UTC) | Action | Finding |
|------------|--------|---------|
| 2024-12-29 | Searched Open-Meteo official docs | Found comprehensive API documentation with all endpoints and parameters |
| 2024-12-29 | Reviewed GitHub repository | Confirmed open-source nature, found client SDK examples |
| 2024-12-29 | Searched for Airbyte connector | No official Airbyte connector found for Open-Meteo |
| 2024-12-29 | Verified API endpoints | Confirmed forecast, archive, and geocoding endpoints work without authentication |
| 2024-12-29 | Analyzed response structure | Confirmed parallel array format requiring pivot transformation |

