# Lakeflow Open-Meteo Community Connector

This documentation describes how to configure and use the **Open-Meteo** Lakeflow community connector to ingest weather data from the Open-Meteo API into Databricks.

## Prerequisites

- **No authentication required**: Open-Meteo is a free, open-source weather API that does not require API keys for non-commercial use.
- **Network access**: The environment running the connector must be able to reach:
  - `https://api.open-meteo.com` (forecasts)
  - `https://archive-api.open-meteo.com` (historical data)
  - `https://geocoding-api.open-meteo.com` (location search)
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Open-Meteo requires **no authentication**, so connection parameters are minimal:

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `base_url` | string | no | Override the base API URL (for testing or custom deployments). Defaults to `https://api.open-meteo.com`. | `https://api.open-meteo.com` |
| `timeout` | string | no | Request timeout in seconds. Defaults to `30`. | `60` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that are allowed to be passed through to the connector. | `latitude,longitude,hourly,daily,forecast_days,start_date,end_date,timezone,temperature_unit,wind_speed_unit,precipitation_unit,name,count,language` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`latitude,longitude,hourly,daily,forecast_days,start_date,end_date,timezone,temperature_unit,wind_speed_unit,precipitation_unit,name,count,language`

> **Note**: Table-specific options such as `latitude`, `longitude`, or `name` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `latitude,longitude,hourly,daily,forecast_days,start_date,end_date,timezone,temperature_unit,wind_speed_unit,precipitation_unit,name,count,language` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Open-Meteo connector exposes a **static list** of tables:

- `forecast`
- `historical`
- `geocoding`

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Keys | Incremental Cursor |
|-------|-------------|----------------|--------------|-------------------|
| `forecast` | Hourly weather forecast for up to 16 days ahead | `snapshot` | `latitude`, `longitude`, `time` | n/a (full refresh) |
| `historical` | Historical weather data from 1940 onwards (ERA5 reanalysis) | `append` | `latitude`, `longitude`, `time` | `time` (via date range) |
| `geocoding` | Location search by name to get coordinates | `snapshot` | `id` | n/a (full refresh) |

### Required and optional table options

#### `forecast` Table

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `latitude` | string | yes | Latitude in decimal degrees (-90 to 90) | `52.52` |
| `longitude` | string | yes | Longitude in decimal degrees (-180 to 180) | `13.41` |
| `hourly` | string | no | Comma-separated list of hourly variables | `temperature_2m,precipitation,weather_code` |
| `forecast_days` | string | no | Number of forecast days (1-16). Default: 7 | `7` |
| `timezone` | string | no | Timezone name for local times | `Europe/Berlin` |
| `temperature_unit` | string | no | `celsius` or `fahrenheit`. Default: `celsius` | `celsius` |
| `wind_speed_unit` | string | no | `kmh`, `ms`, `mph`, or `kn`. Default: `kmh` | `kmh` |
| `precipitation_unit` | string | no | `mm` or `inch`. Default: `mm` | `mm` |

#### `historical` Table

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `latitude` | string | yes | Latitude in decimal degrees | `52.52` |
| `longitude` | string | yes | Longitude in decimal degrees | `13.41` |
| `start_date` | string | yes | Start date (YYYY-MM-DD) | `2024-01-01` |
| `end_date` | string | yes | End date (YYYY-MM-DD) | `2024-01-31` |
| `hourly` | string | no | Comma-separated list of hourly variables | `temperature_2m,precipitation` |
| `timezone` | string | no | Timezone name | `America/New_York` |

#### `geocoding` Table

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `name` | string | yes | Location name to search | `Berlin` |
| `count` | string | no | Number of results (1-100). Default: 10 | `5` |
| `language` | string | no | Language code for results. Default: `en` | `de` |

### Available Weather Variables

Common hourly variables you can request:

| Variable | Description | Unit |
|----------|-------------|------|
| `temperature_2m` | Temperature at 2m above ground | °C |
| `relative_humidity_2m` | Relative humidity at 2m | % |
| `apparent_temperature` | Feels-like temperature | °C |
| `precipitation` | Total precipitation | mm |
| `rain` | Rain amount | mm |
| `snowfall` | Snowfall amount | cm |
| `weather_code` | WMO weather condition code | - |
| `cloud_cover` | Total cloud cover | % |
| `wind_speed_10m` | Wind speed at 10m | km/h |
| `wind_direction_10m` | Wind direction at 10m | ° |
| `wind_gusts_10m` | Wind gusts at 10m | km/h |
| `visibility` | Visibility | m |
| `uv_index` | UV index | - |
| `pressure_msl` | Mean sea level pressure | hPa |

For a complete list, see the [Open-Meteo documentation](https://open-meteo.com/en/docs).

### Weather Code Reference (WMO)

| Code | Description |
|------|-------------|
| 0 | Clear sky |
| 1, 2, 3 | Mainly clear, partly cloudy, overcast |
| 45, 48 | Fog |
| 51, 53, 55 | Drizzle: Light, moderate, dense |
| 61, 63, 65 | Rain: Slight, moderate, heavy |
| 71, 73, 75 | Snow fall: Slight, moderate, heavy |
| 80, 81, 82 | Rain showers: Slight, moderate, violent |
| 95 | Thunderstorm |
| 96, 99 | Thunderstorm with hail |

## Data Type Mapping

| Open-Meteo Type | Spark Type | Notes |
|-----------------|------------|-------|
| float | `DoubleType` | All numeric weather values |
| integer | `LongType` | Weather codes, counts, IDs |
| string (ISO8601) | `StringType` | Timestamps; can be parsed downstream |
| string | `StringType` | Names, codes, timezones |
| array | `ArrayType(StringType)` | Postcodes in geocoding results |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Open-Meteo connector source in your workspace.

### Step 2: Configure Your Pipeline

In your pipeline code, configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this Open-Meteo connector.
- One or more **tables** to ingest, each with required `table_options`.

**Example: Weather forecast for Berlin**

```json
{
  "pipeline_spec": {
    "connection_name": "open_meteo_connection",
    "objects": [
      {
        "table": {
          "source_table": "forecast",
          "latitude": "52.52",
          "longitude": "13.41",
          "hourly": "temperature_2m,precipitation,weather_code,wind_speed_10m",
          "forecast_days": "7",
          "timezone": "Europe/Berlin"
        }
      }
    ]
  }
}
```

**Example: Historical weather data**

```json
{
  "pipeline_spec": {
    "connection_name": "open_meteo_connection",
    "objects": [
      {
        "table": {
          "source_table": "historical",
          "latitude": "40.7128",
          "longitude": "-74.0060",
          "start_date": "2024-01-01",
          "end_date": "2024-01-31",
          "hourly": "temperature_2m,precipitation,snow_depth",
          "timezone": "America/New_York"
        }
      }
    ]
  }
}
```

**Example: Location search**

```json
{
  "pipeline_spec": {
    "connection_name": "open_meteo_connection",
    "objects": [
      {
        "table": {
          "source_table": "geocoding",
          "name": "San Francisco",
          "count": "10"
        }
      }
    ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration.

#### Best Practices

- **Start small**: Begin with a single location and short date range to validate configuration.
- **Use incremental sync for historical data**: Use date ranges to load data incrementally rather than requesting years of data at once.
- **Cache forecast data appropriately**: Forecasts update hourly; schedule syncs based on your freshness requirements.
- **Respect rate limits**: Open-Meteo requests fair use (under 10,000 requests/day for non-commercial use). For higher volumes, contact Open-Meteo for a subscription.
- **Batch locations separately**: The API supports one location per request. For multiple locations, create separate table entries in your pipeline spec.

#### Troubleshooting

**Common Issues:**

- **Missing latitude/longitude**: Ensure both `latitude` and `longitude` are provided for `forecast` and `historical` tables.
- **Invalid date format**: Use `YYYY-MM-DD` format for `start_date` and `end_date`.
- **No results for geocoding**: Try a more specific location name or check spelling.
- **Historical data delay**: ERA5 historical data has approximately a 5-day delay from the present.
- **Rate limiting**: If you exceed fair use limits, reduce request frequency or contact Open-Meteo for commercial access.

## References

- Connector implementation: `sources/open_meteo/open_meteo.py`
- Connector API documentation: `sources/open_meteo/open_meteo_api_doc.md`
- Official Open-Meteo documentation:
  - https://open-meteo.com/en/docs (Forecast API)
  - https://open-meteo.com/en/docs/historical-weather-api (Historical API)
  - https://open-meteo.com/en/docs/geocoding-api (Geocoding API)
- Open-Meteo GitHub: https://github.com/open-meteo/open-meteo
- Data License: CC BY 4.0 (Attribution required)

