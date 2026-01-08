import requests
from typing import Dict, List, Tuple, Iterator, Any
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    DoubleType,
    ArrayType,
)


class LakeflowConnect:
    """
    Google Maps API connector for Lakeflow.
    
    This connector supports:
    - Places API (New) Text Search for place data
    - Geocoding API for address-to-coordinates and reverse geocoding
    - Distance Matrix API for travel distance and time calculations
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the Google Maps connector.

        Args:
            options: Dictionary containing:
                - api_key: Google Maps Platform API key
        """
        self.api_key = options["api_key"]
        self.places_base_url = "https://places.googleapis.com/v1"
        self.maps_base_url = "https://maps.googleapis.com/maps/api"
        
        # Default field mask for Places API requests
        self._default_field_mask = ",".join([
            "places.id",
            "places.displayName",
            "places.formattedAddress",
            "places.shortFormattedAddress",
            "places.addressComponents",
            "places.location",
            "places.viewport",
            "places.googleMapsUri",
            "places.websiteUri",
            "places.internationalPhoneNumber",
            "places.nationalPhoneNumber",
            "places.types",
            "places.primaryType",
            "places.primaryTypeDisplayName",
            "places.businessStatus",
            "places.priceLevel",
            "places.rating",
            "places.userRatingCount",
            "places.currentOpeningHours",
            "places.regularOpeningHours",
            "places.utcOffsetMinutes",
            "places.editorialSummary",
            "places.iconMaskBaseUri",
            "places.iconBackgroundColor",
            "places.takeout",
            "places.delivery",
            "places.dineIn",
            "places.reservable",
            "places.servesBreakfast",
            "places.servesLunch",
            "places.servesDinner",
            "places.servesBeer",
            "places.servesWine",
            "places.servesBrunch",
            "places.servesVegetarianFood",
            "places.outdoorSeating",
            "places.liveMusic",
            "places.menuForChildren",
            "places.goodForChildren",
            "places.allowsDogs",
            "places.goodForGroups",
            "places.goodForWatchingSports",
            "places.accessibilityOptions",
            "places.parkingOptions",
            "places.paymentOptions",
            "places.plusCode",
            "places.adrFormatAddress",
            "nextPageToken",
        ])

        # =====================
        # Places Schema
        # =====================
        
        # Nested schema for displayName
        self._display_name_schema = StructType([
            StructField("text", StringType(), True),
            StructField("languageCode", StringType(), True),
        ])

        # Nested schema for location (Places API format)
        self._places_location_schema = StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ])

        # Nested schema for viewport (Places API format)
        self._places_viewport_schema = StructType([
            StructField("low", self._places_location_schema, True),
            StructField("high", self._places_location_schema, True),
        ])

        # Nested schema for address component (Places API format)
        self._places_address_component_schema = StructType([
            StructField("longText", StringType(), True),
            StructField("shortText", StringType(), True),
            StructField("types", ArrayType(StringType()), True),
            StructField("languageCode", StringType(), True),
        ])

        # Nested schema for opening hours period point
        self._period_point_schema = StructType([
            StructField("day", LongType(), True),
            StructField("hour", LongType(), True),
            StructField("minute", LongType(), True),
        ])

        # Nested schema for opening hours period
        self._period_schema = StructType([
            StructField("open", self._period_point_schema, True),
            StructField("close", self._period_point_schema, True),
        ])

        # Nested schema for opening hours
        self._opening_hours_schema = StructType([
            StructField("openNow", BooleanType(), True),
            StructField("periods", ArrayType(self._period_schema), True),
            StructField("weekdayDescriptions", ArrayType(StringType()), True),
        ])

        # Nested schema for editorial summary
        self._editorial_summary_schema = StructType([
            StructField("text", StringType(), True),
            StructField("languageCode", StringType(), True),
        ])

        # Nested schema for accessibility options
        self._accessibility_options_schema = StructType([
            StructField("wheelchairAccessibleParking", BooleanType(), True),
            StructField("wheelchairAccessibleEntrance", BooleanType(), True),
            StructField("wheelchairAccessibleRestroom", BooleanType(), True),
            StructField("wheelchairAccessibleSeating", BooleanType(), True),
        ])

        # Nested schema for parking options
        self._parking_options_schema = StructType([
            StructField("freeParking", BooleanType(), True),
            StructField("paidParking", BooleanType(), True),
            StructField("freeStreetParking", BooleanType(), True),
            StructField("paidStreetParking", BooleanType(), True),
            StructField("valetParking", BooleanType(), True),
            StructField("freeGarageParking", BooleanType(), True),
            StructField("paidGarageParking", BooleanType(), True),
        ])

        # Nested schema for payment options
        self._payment_options_schema = StructType([
            StructField("acceptsCreditCards", BooleanType(), True),
            StructField("acceptsDebitCards", BooleanType(), True),
            StructField("acceptsCashOnly", BooleanType(), True),
            StructField("acceptsNfc", BooleanType(), True),
        ])

        # Nested schema for plus code (Places API format)
        self._places_plus_code_schema = StructType([
            StructField("globalCode", StringType(), True),
            StructField("compoundCode", StringType(), True),
        ])

        # Full places schema
        self._places_schema = StructType([
            StructField("id", StringType(), False),
            StructField("displayName", self._display_name_schema, True),
            StructField("formattedAddress", StringType(), True),
            StructField("shortFormattedAddress", StringType(), True),
            StructField("addressComponents", ArrayType(self._places_address_component_schema), True),
            StructField("location", self._places_location_schema, True),
            StructField("viewport", self._places_viewport_schema, True),
            StructField("googleMapsUri", StringType(), True),
            StructField("websiteUri", StringType(), True),
            StructField("internationalPhoneNumber", StringType(), True),
            StructField("nationalPhoneNumber", StringType(), True),
            StructField("types", ArrayType(StringType()), True),
            StructField("primaryType", StringType(), True),
            StructField("primaryTypeDisplayName", self._display_name_schema, True),
            StructField("businessStatus", StringType(), True),
            StructField("priceLevel", StringType(), True),
            StructField("rating", DoubleType(), True),
            StructField("userRatingCount", LongType(), True),
            StructField("currentOpeningHours", self._opening_hours_schema, True),
            StructField("regularOpeningHours", self._opening_hours_schema, True),
            StructField("utcOffsetMinutes", LongType(), True),
            StructField("editorialSummary", self._editorial_summary_schema, True),
            StructField("iconMaskBaseUri", StringType(), True),
            StructField("iconBackgroundColor", StringType(), True),
            StructField("takeout", BooleanType(), True),
            StructField("delivery", BooleanType(), True),
            StructField("dineIn", BooleanType(), True),
            StructField("reservable", BooleanType(), True),
            StructField("servesBreakfast", BooleanType(), True),
            StructField("servesLunch", BooleanType(), True),
            StructField("servesDinner", BooleanType(), True),
            StructField("servesBeer", BooleanType(), True),
            StructField("servesWine", BooleanType(), True),
            StructField("servesBrunch", BooleanType(), True),
            StructField("servesVegetarianFood", BooleanType(), True),
            StructField("outdoorSeating", BooleanType(), True),
            StructField("liveMusic", BooleanType(), True),
            StructField("menuForChildren", BooleanType(), True),
            StructField("goodForChildren", BooleanType(), True),
            StructField("allowsDogs", BooleanType(), True),
            StructField("goodForGroups", BooleanType(), True),
            StructField("goodForWatchingSports", BooleanType(), True),
            StructField("accessibilityOptions", self._accessibility_options_schema, True),
            StructField("parkingOptions", self._parking_options_schema, True),
            StructField("paymentOptions", self._payment_options_schema, True),
            StructField("plusCode", self._places_plus_code_schema, True),
            StructField("adrFormatAddress", StringType(), True),
        ])

        # =====================
        # Geocoder Schema
        # =====================
        
        # Nested schema for geocoder location (lat/lng format)
        self._geocoder_location_schema = StructType([
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True),
        ])

        # Nested schema for geocoder viewport bounds
        self._geocoder_bounds_schema = StructType([
            StructField("northeast", self._geocoder_location_schema, True),
            StructField("southwest", self._geocoder_location_schema, True),
        ])

        # Nested schema for geocoder geometry
        self._geocoder_geometry_schema = StructType([
            StructField("location", self._geocoder_location_schema, True),
            StructField("location_type", StringType(), True),
            StructField("viewport", self._geocoder_bounds_schema, True),
            StructField("bounds", self._geocoder_bounds_schema, True),
        ])

        # Nested schema for geocoder address component
        self._geocoder_address_component_schema = StructType([
            StructField("long_name", StringType(), True),
            StructField("short_name", StringType(), True),
            StructField("types", ArrayType(StringType()), True),
        ])

        # Nested schema for geocoder plus code
        self._geocoder_plus_code_schema = StructType([
            StructField("global_code", StringType(), True),
            StructField("compound_code", StringType(), True),
        ])

        # Full geocoder result schema
        self._geocoder_schema = StructType([
            StructField("place_id", StringType(), False),
            StructField("formatted_address", StringType(), True),
            StructField("address_components", ArrayType(self._geocoder_address_component_schema), True),
            StructField("geometry", self._geocoder_geometry_schema, True),
            StructField("types", ArrayType(StringType()), True),
            StructField("partial_match", BooleanType(), True),
            StructField("plus_code", self._geocoder_plus_code_schema, True),
            StructField("postcode_localities", ArrayType(StringType()), True),
        ])

        # =====================
        # Distance Matrix Schema
        # =====================
        
        # Nested schema for distance/duration value
        self._distance_value_schema = StructType([
            StructField("value", LongType(), True),
            StructField("text", StringType(), True),
        ])

        # Nested schema for fare
        self._fare_schema = StructType([
            StructField("currency", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("text", StringType(), True),
        ])

        # Nested schema for matrix element
        self._matrix_element_schema = StructType([
            StructField("status", StringType(), True),
            StructField("distance", self._distance_value_schema, True),
            StructField("duration", self._distance_value_schema, True),
            StructField("duration_in_traffic", self._distance_value_schema, True),
            StructField("fare", self._fare_schema, True),
        ])

        # Full distance matrix result schema (flattened per origin-destination pair)
        self._distance_matrix_schema = StructType([
            StructField("origin_index", LongType(), False),
            StructField("destination_index", LongType(), False),
            StructField("origin_address", StringType(), True),
            StructField("destination_address", StringType(), True),
            StructField("status", StringType(), True),
            StructField("distance", self._distance_value_schema, True),
            StructField("duration", self._distance_value_schema, True),
            StructField("duration_in_traffic", self._distance_value_schema, True),
            StructField("fare", self._fare_schema, True),
        ])

        # =====================
        # Object Configuration
        # =====================
        self._object_config = {
            "places": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "geocoder": {
                "primary_keys": ["place_id"],
                "ingestion_type": "snapshot",
            },
            "distance_matrix": {
                "primary_keys": ["origin_index", "destination_index"],
                "ingestion_type": "snapshot",
            },
        }

        # Schema mapping
        self._schema_config = {
            "places": self._places_schema,
            "geocoder": self._geocoder_schema,
            "distance_matrix": self._distance_matrix_schema,
        }

    def list_tables(self) -> List[str]:
        """
        List available tables/objects.

        Returns:
            List of supported table names
        """
        return ["places", "geocoder", "distance_matrix"]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Get the Spark schema for a table.

        Args:
            table_name: Name of the table
            table_options: Additional options (not used for schema)

        Returns:
            StructType representing the table schema
        """
        if table_name not in self._schema_config:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.list_tables()}"
            )
        return self._schema_config[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Get metadata for a table.

        Args:
            table_name: Name of the table
            table_options: Additional options (not used for metadata)

        Returns:
            Dictionary with primary_keys and ingestion_type
        """
        if table_name not in self._object_config:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.list_tables()}"
            )
        config = self._object_config[table_name]
        return {
            "primary_keys": config["primary_keys"],
            "ingestion_type": config["ingestion_type"],
        }

    def read_table(
        self, table_name: str, start_offset: Dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """
        Read data from a table.

        Args:
            table_name: Name of the table to read
            start_offset: Not used for snapshot ingestion
            table_options: Dictionary containing table-specific options

        Returns:
            Tuple of (iterator of records, offset dict)
        """
        if table_name not in self._object_config:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.list_tables()}"
            )

        if table_name == "places":
            return self._read_places(table_options)
        elif table_name == "geocoder":
            return self._read_geocoder(table_options)
        elif table_name == "distance_matrix":
            return self._read_distance_matrix(table_options)
        else:
            raise ValueError(f"Unsupported table: {table_name}")

    def _read_places(
        self, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """
        Read data from the places table using Text Search API.

        Args:
            table_options: Dictionary containing:
                - text_query: Required. The search query (e.g., "restaurants in Seattle")
                - language_code: Optional. Language code for results (e.g., "en")
                - max_result_count: Optional. Maximum results per page (1-20, default 20)
                - included_type: Optional. Restrict to specific place type
                - min_rating: Optional. Minimum average rating filter
                - open_now: Optional. Only return places open now ("true"/"false")
                - region_code: Optional. Region code for biasing (e.g., "US")

        Returns:
            Tuple of (iterator of place records, empty offset dict)
        """
        # Validate required parameter
        text_query = table_options.get("text_query")
        if not text_query:
            raise ValueError(
                "table_options must include 'text_query' parameter for the places table"
            )

        # Build request parameters
        request_body = {
            "textQuery": text_query,
        }

        # Add optional parameters
        if "language_code" in table_options:
            request_body["languageCode"] = table_options["language_code"]
        
        if "max_result_count" in table_options:
            request_body["maxResultCount"] = int(table_options["max_result_count"])
        
        if "included_type" in table_options:
            request_body["includedType"] = table_options["included_type"]
        
        if "min_rating" in table_options:
            request_body["minRating"] = float(table_options["min_rating"])
        
        if "open_now" in table_options:
            request_body["openNow"] = table_options["open_now"].lower() == "true"
        
        if "region_code" in table_options:
            request_body["regionCode"] = table_options["region_code"]

        # Create iterator that yields places
        def places_iterator() -> Iterator[Dict]:
            current_body = request_body.copy()
            page_count = 0
            max_pages = 3  # Google Places API allows max 60 results (3 pages of 20)

            while page_count < max_pages:
                # Make API request
                response = requests.post(
                    f"{self.places_base_url}/places:searchText",
                    headers={
                        "Content-Type": "application/json",
                        "X-Goog-Api-Key": self.api_key,
                        "X-Goog-FieldMask": self._default_field_mask,
                    },
                    json=current_body,
                )

                if response.status_code != 200:
                    raise Exception(
                        f"Google Places API error: {response.status_code} {response.text}"
                    )

                data = response.json()
                places = data.get("places", [])

                # Yield each place
                for place in places:
                    yield place

                # Check for next page
                next_page_token = data.get("nextPageToken")
                if not next_page_token:
                    break

                # Update request for next page
                current_body["pageToken"] = next_page_token
                page_count += 1

        return places_iterator(), {}

    def _read_geocoder(
        self, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """
        Read data from the geocoder table using Geocoding API.

        Args:
            table_options: Dictionary containing:
                - address: Address to geocode (for forward geocoding)
                - latlng: Coordinates to reverse geocode, format: "lat,lng"
                - place_id: Place ID to geocode
                (One of address, latlng, or place_id is required)
                - language: Optional. Language code for results (e.g., "en")
                - region: Optional. Region code to bias results (e.g., "us")
                - bounds: Optional. Bounding box to bias results, format: "lat,lng|lat,lng"
                - components: Optional. Component filter, format: "component:value|..."

        Returns:
            Tuple of (iterator of geocoder results, empty offset dict)
        """
        # Build request parameters
        params = {
            "key": self.api_key,
        }

        # Validate that at least one of address, latlng, or place_id is provided
        if "address" in table_options:
            params["address"] = table_options["address"]
        elif "latlng" in table_options:
            params["latlng"] = table_options["latlng"]
        elif "place_id" in table_options:
            params["place_id"] = table_options["place_id"]
        else:
            raise ValueError(
                "table_options must include one of 'address', 'latlng', or 'place_id' "
                "for the geocoder table"
            )

        # Add optional parameters
        if "language" in table_options:
            params["language"] = table_options["language"]
        
        if "region" in table_options:
            params["region"] = table_options["region"]
        
        if "bounds" in table_options:
            params["bounds"] = table_options["bounds"]
        
        if "components" in table_options:
            params["components"] = table_options["components"]

        # For reverse geocoding, additional filters
        if "result_type" in table_options:
            params["result_type"] = table_options["result_type"]
        
        if "location_type" in table_options:
            params["location_type"] = table_options["location_type"]

        # Create iterator that yields geocoder results
        def geocoder_iterator() -> Iterator[Dict]:
            response = requests.get(
                f"{self.maps_base_url}/geocode/json",
                params=params,
            )

            if response.status_code != 200:
                raise Exception(
                    f"Google Geocoding API error: {response.status_code} {response.text}"
                )

            data = response.json()
            status = data.get("status")

            if status == "ZERO_RESULTS":
                # No results, return empty iterator
                return
            elif status != "OK":
                raise Exception(
                    f"Google Geocoding API error: status={status}, "
                    f"error_message={data.get('error_message', 'Unknown error')}"
                )

            results = data.get("results", [])

            for result in results:
                yield result

        return geocoder_iterator(), {}

    def _read_distance_matrix(
        self, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """
        Read data from the distance_matrix table using Distance Matrix API.

        Args:
            table_options: Dictionary containing:
                - origins: Required. Pipe-separated list of origins
                - destinations: Required. Pipe-separated list of destinations
                - mode: Optional. Travel mode: driving, walking, bicycling, transit
                - language: Optional. Language code for results
                - region: Optional. Region code to bias results
                - avoid: Optional. Features to avoid: tolls|highways|ferries|indoor
                - units: Optional. Unit system: metric or imperial
                - departure_time: Optional. Departure time as Unix timestamp or "now"
                - arrival_time: Optional. Arrival time as Unix timestamp (transit only)
                - traffic_model: Optional. Traffic model: best_guess, pessimistic, optimistic
                - transit_mode: Optional. Transit modes: bus|subway|train|tram|rail
                - transit_routing_preference: Optional. less_walking or fewer_transfers

        Returns:
            Tuple of (iterator of distance matrix results, empty offset dict)
        """
        # Validate required parameters
        origins = table_options.get("origins")
        destinations = table_options.get("destinations")

        if not origins:
            raise ValueError(
                "table_options must include 'origins' parameter for the distance_matrix table"
            )
        if not destinations:
            raise ValueError(
                "table_options must include 'destinations' parameter for the distance_matrix table"
            )

        # Build request parameters
        params = {
            "key": self.api_key,
            "origins": origins,
            "destinations": destinations,
        }

        # Add optional parameters
        if "mode" in table_options:
            params["mode"] = table_options["mode"]
        
        if "language" in table_options:
            params["language"] = table_options["language"]
        
        if "region" in table_options:
            params["region"] = table_options["region"]
        
        if "avoid" in table_options:
            params["avoid"] = table_options["avoid"]
        
        if "units" in table_options:
            params["units"] = table_options["units"]
        
        if "departure_time" in table_options:
            params["departure_time"] = table_options["departure_time"]
        
        if "arrival_time" in table_options:
            params["arrival_time"] = table_options["arrival_time"]
        
        if "traffic_model" in table_options:
            params["traffic_model"] = table_options["traffic_model"]
        
        if "transit_mode" in table_options:
            params["transit_mode"] = table_options["transit_mode"]
        
        if "transit_routing_preference" in table_options:
            params["transit_routing_preference"] = table_options["transit_routing_preference"]

        # Create iterator that yields distance matrix results
        def distance_matrix_iterator() -> Iterator[Dict]:
            response = requests.get(
                f"{self.maps_base_url}/distancematrix/json",
                params=params,
            )

            if response.status_code != 200:
                raise Exception(
                    f"Google Distance Matrix API error: {response.status_code} {response.text}"
                )

            data = response.json()
            status = data.get("status")

            if status != "OK":
                raise Exception(
                    f"Google Distance Matrix API error: status={status}, "
                    f"error_message={data.get('error_message', 'Unknown error')}"
                )

            origin_addresses = data.get("origin_addresses", [])
            destination_addresses = data.get("destination_addresses", [])
            rows = data.get("rows", [])

            # Flatten the matrix into individual records
            for origin_index, row in enumerate(rows):
                elements = row.get("elements", [])
                for destination_index, element in enumerate(elements):
                    # Build flattened record
                    record = {
                        "origin_index": origin_index,
                        "destination_index": destination_index,
                        "origin_address": origin_addresses[origin_index] if origin_index < len(origin_addresses) else None,
                        "destination_address": destination_addresses[destination_index] if destination_index < len(destination_addresses) else None,
                        "status": element.get("status"),
                        "distance": element.get("distance"),
                        "duration": element.get("duration"),
                        "duration_in_traffic": element.get("duration_in_traffic"),
                        "fare": element.get("fare"),
                    }
                    yield record

        return distance_matrix_iterator(), {}

    def test_connection(self) -> Dict[str, str]:
        """
        Test the connection to Google Maps APIs.

        Returns:
            Dictionary with status and message
        """
        try:
            # Test Places API
            response = requests.post(
                f"{self.places_base_url}/places:searchText",
                headers={
                    "Content-Type": "application/json",
                    "X-Goog-Api-Key": self.api_key,
                    "X-Goog-FieldMask": "places.id",
                },
                json={
                    "textQuery": "test",
                    "maxResultCount": 1,
                },
            )

            if response.status_code == 200:
                return {"status": "success", "message": "Connection successful"}
            else:
                return {
                    "status": "error",
                    "message": f"API error: {response.status_code} {response.text}",
                }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}
