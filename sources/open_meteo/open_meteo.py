import requests
from typing import Dict, List, Iterator, Any, Optional
from datetime import datetime, timedelta

from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    LongType,
    StringType,
    ArrayType,
)


class LakeflowConnect:
    """
    Open-Meteo connector for weather forecast and historical data.
    
    Open-Meteo is a free weather API that provides:
    - Weather forecasts up to 16 days ahead
    - Historical weather data (80+ years)
    - Geocoding for location search
    
    No authentication required for non-commercial use.
    """

    # Supported tables
    TABLES = ["forecast", "historical", "geocoding"]

    # API base URLs
    FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"
    ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"
    GEOCODING_API_URL = "https://geocoding-api.open-meteo.com/v1/search"

    # Default hourly variables to fetch if none specified
    DEFAULT_HOURLY_VARIABLES = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "weather_code",
        "cloud_cover",
        "wind_speed_10m",
        "wind_direction_10m",
    ]

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the Open-Meteo connector.
        
        Args:
            options: Connection options. Optional keys:
                - base_url: Override the base API URL (for testing)
                - timeout: Request timeout in seconds (default: 30)
        """
        self.options = options
        self.base_url = options.get("base_url", "https://api.open-meteo.com")
        
        try:
            self.timeout = int(options.get("timeout", "30"))
        except (TypeError, ValueError):
            self.timeout = 30
        
        # Create a session for connection pooling
        self._session = requests.Session()

    def list_tables(self) -> List[str]:
        """
        Return the list of available tables.
        
        Returns:
            List of table names: forecast, historical, geocoding
        """
        return self.TABLES.copy()

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Return the schema for a table.
        
        Args:
            table_name: Name of the table
            table_options: Additional options (not used for schema)
            
        Returns:
            StructType schema for the table
        """
        if table_name not in self.TABLES:
            raise ValueError(f"Unsupported table: {table_name!r}. Supported tables: {self.TABLES}")

        if table_name == "forecast":
            return self._get_weather_schema()
        elif table_name == "historical":
            return self._get_weather_schema()
        elif table_name == "geocoding":
            return self._get_geocoding_schema()
        
        raise ValueError(f"Unsupported table: {table_name!r}")

    def _get_weather_schema(self) -> StructType:
        """
        Return the schema for weather data (forecast and historical).
        
        The schema includes location metadata and all common hourly variables.
        """
        return StructType([
            # Location and time identifiers
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("time", StringType(), False),
            
            # Location metadata
            StructField("elevation", DoubleType(), True),
            StructField("timezone", StringType(), True),
            StructField("timezone_abbreviation", StringType(), True),
            
            # Temperature variables
            StructField("temperature_2m", DoubleType(), True),
            StructField("apparent_temperature", DoubleType(), True),
            StructField("dewpoint_2m", DoubleType(), True),
            
            # Humidity
            StructField("relative_humidity_2m", DoubleType(), True),
            
            # Precipitation
            StructField("precipitation", DoubleType(), True),
            StructField("rain", DoubleType(), True),
            StructField("showers", DoubleType(), True),
            StructField("snowfall", DoubleType(), True),
            StructField("snow_depth", DoubleType(), True),
            
            # Weather condition
            StructField("weather_code", LongType(), True),
            
            # Cloud cover
            StructField("cloud_cover", DoubleType(), True),
            StructField("cloud_cover_low", DoubleType(), True),
            StructField("cloud_cover_mid", DoubleType(), True),
            StructField("cloud_cover_high", DoubleType(), True),
            
            # Pressure
            StructField("pressure_msl", DoubleType(), True),
            StructField("surface_pressure", DoubleType(), True),
            
            # Wind
            StructField("wind_speed_10m", DoubleType(), True),
            StructField("wind_direction_10m", DoubleType(), True),
            StructField("wind_gusts_10m", DoubleType(), True),
            
            # Visibility and UV
            StructField("visibility", DoubleType(), True),
            StructField("uv_index", DoubleType(), True),
            StructField("uv_index_clear_sky", DoubleType(), True),
            
            # Day/night and sunshine
            StructField("is_day", LongType(), True),
            StructField("sunshine_duration", DoubleType(), True),
            
            # Radiation
            StructField("shortwave_radiation", DoubleType(), True),
            StructField("direct_radiation", DoubleType(), True),
            StructField("diffuse_radiation", DoubleType(), True),
            
            # Evapotranspiration
            StructField("et0_fao_evapotranspiration", DoubleType(), True),
        ])

    def _get_geocoding_schema(self) -> StructType:
        """Return the schema for geocoding results."""
        return StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("elevation", DoubleType(), True),
            StructField("feature_code", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("admin1", StringType(), True),
            StructField("admin2", StringType(), True),
            StructField("admin3", StringType(), True),
            StructField("admin4", StringType(), True),
            StructField("timezone", StringType(), True),
            StructField("population", LongType(), True),
            StructField("postcodes", ArrayType(StringType(), True), True),
        ])

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Return metadata for a table.
        
        Args:
            table_name: Name of the table
            table_options: Additional options (not used)
            
        Returns:
            Dict with primary_keys, cursor_field (if applicable), and ingestion_type
        """
        if table_name not in self.TABLES:
            raise ValueError(f"Unsupported table: {table_name!r}. Supported tables: {self.TABLES}")

        if table_name == "forecast":
            return {
                "primary_keys": ["latitude", "longitude", "time"],
                "ingestion_type": "snapshot",
            }
        elif table_name == "historical":
            return {
                "primary_keys": ["latitude", "longitude", "time"],
                "ingestion_type": "append",
            }
        elif table_name == "geocoding":
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }
        
        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple:
        """
        Read data from a table.
        
        Args:
            table_name: Name of the table to read
            start_offset: Starting offset for incremental reads (used for historical)
            table_options: Table-specific options:
                - For forecast/historical:
                    - latitude (required): Latitude in decimal degrees
                    - longitude (required): Longitude in decimal degrees
                    - hourly: Comma-separated list of variables (optional)
                    - forecast_days: Number of forecast days (forecast only, default: 7)
                    - start_date: Start date YYYY-MM-DD (historical only)
                    - end_date: End date YYYY-MM-DD (historical only)
                    - timezone: Timezone name (optional)
                - For geocoding:
                    - name (required): Location name to search
                    - count: Number of results (default: 10)
                    - language: Language code (default: en)
                    
        Returns:
            Tuple of (Iterator[dict], dict) - records iterator and next offset
        """
        if table_name not in self.TABLES:
            raise ValueError(f"Unsupported table: {table_name!r}. Supported tables: {self.TABLES}")

        if table_name == "forecast":
            return self._read_forecast(start_offset, table_options)
        elif table_name == "historical":
            return self._read_historical(start_offset, table_options)
        elif table_name == "geocoding":
            return self._read_geocoding(start_offset, table_options)
        
        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_forecast(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple:
        """Read weather forecast data."""
        latitude = table_options.get("latitude")
        longitude = table_options.get("longitude")
        
        if not latitude or not longitude:
            raise ValueError(
                "table_options for 'forecast' must include 'latitude' and 'longitude'"
            )
        
        # Parse hourly variables
        hourly_str = table_options.get("hourly")
        if hourly_str:
            hourly_vars = [v.strip() for v in hourly_str.split(",") if v.strip()]
        else:
            hourly_vars = self.DEFAULT_HOURLY_VARIABLES.copy()
        
        # Build request parameters
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(hourly_vars),
            "timeformat": "iso8601",
        }
        
        # Optional parameters
        if table_options.get("forecast_days"):
            params["forecast_days"] = table_options["forecast_days"]
        if table_options.get("timezone"):
            params["timezone"] = table_options["timezone"]
        if table_options.get("temperature_unit"):
            params["temperature_unit"] = table_options["temperature_unit"]
        if table_options.get("wind_speed_unit"):
            params["wind_speed_unit"] = table_options["wind_speed_unit"]
        if table_options.get("precipitation_unit"):
            params["precipitation_unit"] = table_options["precipitation_unit"]
        
        # Make API request
        response = self._session.get(
            self.FORECAST_API_URL,
            params=params,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            raise RuntimeError(
                f"Open-Meteo API error: {response.status_code} {response.text}"
            )
        
        data = response.json()
        records = self._pivot_weather_response(data)
        
        # Forecast is snapshot - no incremental offset
        return iter(records), {}

    def _read_historical(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple:
        """Read historical weather data."""
        latitude = table_options.get("latitude")
        longitude = table_options.get("longitude")
        
        if not latitude or not longitude:
            raise ValueError(
                "table_options for 'historical' must include 'latitude' and 'longitude'"
            )
        
        # Determine date range
        start_date = table_options.get("start_date")
        end_date = table_options.get("end_date")
        
        # Use offset for incremental reads if available
        if start_offset and isinstance(start_offset, dict):
            cursor_date = start_offset.get("cursor")
            if cursor_date:
                start_date = cursor_date
        
        if not start_date or not end_date:
            raise ValueError(
                "table_options for 'historical' must include 'start_date' and 'end_date' (YYYY-MM-DD)"
            )
        
        # Parse hourly variables
        hourly_str = table_options.get("hourly")
        if hourly_str:
            hourly_vars = [v.strip() for v in hourly_str.split(",") if v.strip()]
        else:
            hourly_vars = self.DEFAULT_HOURLY_VARIABLES.copy()
        
        # Build request parameters
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(hourly_vars),
            "timeformat": "iso8601",
        }
        
        # Optional parameters
        if table_options.get("timezone"):
            params["timezone"] = table_options["timezone"]
        if table_options.get("temperature_unit"):
            params["temperature_unit"] = table_options["temperature_unit"]
        if table_options.get("wind_speed_unit"):
            params["wind_speed_unit"] = table_options["wind_speed_unit"]
        if table_options.get("precipitation_unit"):
            params["precipitation_unit"] = table_options["precipitation_unit"]
        
        # Make API request
        response = self._session.get(
            self.ARCHIVE_API_URL,
            params=params,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            raise RuntimeError(
                f"Open-Meteo Archive API error: {response.status_code} {response.text}"
            )
        
        data = response.json()
        records = self._pivot_weather_response(data)
        
        # Calculate next offset for incremental reads
        # Use end_date + 1 day as the next cursor
        next_cursor = end_date
        if records:
            try:
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                next_dt = end_dt + timedelta(days=1)
                next_cursor = next_dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
        
        next_offset = {"cursor": next_cursor}
        
        # If we got no records and had a start_offset, return same offset to signal end
        if not records and start_offset:
            next_offset = start_offset
        
        return iter(records), next_offset

    def _read_geocoding(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple:
        """Read geocoding search results."""
        name = table_options.get("name")
        
        if not name:
            raise ValueError(
                "table_options for 'geocoding' must include 'name' (location to search)"
            )
        
        # Build request parameters
        params = {
            "name": name,
            "format": "json",
        }
        
        if table_options.get("count"):
            params["count"] = table_options["count"]
        if table_options.get("language"):
            params["language"] = table_options["language"]
        
        # Make API request
        response = self._session.get(
            self.GEOCODING_API_URL,
            params=params,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            raise RuntimeError(
                f"Open-Meteo Geocoding API error: {response.status_code} {response.text}"
            )
        
        data = response.json()
        results = data.get("results", [])
        
        # Transform results to match schema
        records = []
        for result in results:
            record = {
                "id": result.get("id"),
                "name": result.get("name"),
                "latitude": result.get("latitude"),
                "longitude": result.get("longitude"),
                "elevation": result.get("elevation"),
                "feature_code": result.get("feature_code"),
                "country_code": result.get("country_code"),
                "country": result.get("country"),
                "admin1": result.get("admin1"),
                "admin2": result.get("admin2"),
                "admin3": result.get("admin3"),
                "admin4": result.get("admin4"),
                "timezone": result.get("timezone"),
                "population": result.get("population"),
                "postcodes": result.get("postcodes"),
            }
            records.append(record)
        
        # Geocoding is snapshot - no incremental offset
        return iter(records), {}

    def _pivot_weather_response(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transform Open-Meteo's parallel array response into row-based records.
        
        Open-Meteo returns data like:
        {
            "hourly": {
                "time": ["2024-01-01T00:00", "2024-01-01T01:00", ...],
                "temperature_2m": [2.5, 2.3, ...],
                "precipitation": [0.0, 0.1, ...]
            }
        }
        
        We transform this into:
        [
            {"time": "2024-01-01T00:00", "temperature_2m": 2.5, "precipitation": 0.0, ...},
            {"time": "2024-01-01T01:00", "temperature_2m": 2.3, "precipitation": 0.1, ...},
            ...
        ]
        """
        records = []
        
        # Extract location metadata
        latitude = data.get("latitude")
        longitude = data.get("longitude")
        elevation = data.get("elevation")
        timezone = data.get("timezone")
        timezone_abbr = data.get("timezone_abbreviation")
        
        hourly = data.get("hourly", {})
        if not hourly:
            return records
        
        times = hourly.get("time", [])
        if not times:
            return records
        
        # Get all variable names except 'time'
        variable_names = [k for k in hourly.keys() if k != "time"]
        
        # Pivot: create one record per timestamp
        for i, time_val in enumerate(times):
            record = {
                "latitude": latitude,
                "longitude": longitude,
                "time": time_val,
                "elevation": elevation,
                "timezone": timezone,
                "timezone_abbreviation": timezone_abbr,
            }
            
            # Add each variable's value at this index
            for var_name in variable_names:
                var_values = hourly.get(var_name, [])
                if i < len(var_values):
                    record[var_name] = var_values[i]
                else:
                    record[var_name] = None
            
            records.append(record)
        
        return records

