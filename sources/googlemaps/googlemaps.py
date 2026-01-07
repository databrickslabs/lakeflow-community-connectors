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
    Google Maps Places API connector for Lakeflow.
    
    This connector uses the Places API (New) Text Search endpoint to retrieve
    place data based on text queries.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the Google Maps Places connector.

        Args:
            options: Dictionary containing:
                - api_key: Google Maps Platform API key
        """
        self.api_key = options["api_key"]
        self.base_url = "https://places.googleapis.com/v1"
        
        # Default field mask for the API requests - requesting most useful fields
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

        # Nested schema for displayName
        self._display_name_schema = StructType([
            StructField("text", StringType(), True),
            StructField("languageCode", StringType(), True),
        ])

        # Nested schema for location
        self._location_schema = StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ])

        # Nested schema for viewport
        self._viewport_schema = StructType([
            StructField("low", self._location_schema, True),
            StructField("high", self._location_schema, True),
        ])

        # Nested schema for address component
        self._address_component_schema = StructType([
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

        # Nested schema for plus code
        self._plus_code_schema = StructType([
            StructField("globalCode", StringType(), True),
            StructField("compoundCode", StringType(), True),
        ])

        # Full places schema
        self._places_schema = StructType([
            StructField("id", StringType(), False),
            StructField("displayName", self._display_name_schema, True),
            StructField("formattedAddress", StringType(), True),
            StructField("shortFormattedAddress", StringType(), True),
            StructField("addressComponents", ArrayType(self._address_component_schema), True),
            StructField("location", self._location_schema, True),
            StructField("viewport", self._viewport_schema, True),
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
            StructField("plusCode", self._plus_code_schema, True),
            StructField("adrFormatAddress", StringType(), True),
        ])

        # Object configuration
        self._object_config = {
            "places": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",  # No incremental support for Places API
            },
        }

    def list_tables(self) -> List[str]:
        """
        List available tables/objects.

        Returns:
            List of supported table names
        """
        return ["places"]

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
        if table_name != "places":
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.list_tables()}"
            )
        return self._places_schema

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
        Read data from the places table using Text Search API.

        Args:
            table_name: Name of the table to read (must be "places")
            start_offset: Not used for snapshot ingestion (Places API doesn't support incremental)
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
        if table_name != "places":
            raise ValueError(f"Unsupported table: {table_name}")

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
                    f"{self.base_url}/places:searchText",
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

        # For snapshot ingestion, offset is empty
        return places_iterator(), {}

    def test_connection(self) -> Dict[str, str]:
        """
        Test the connection to Google Places API.

        Returns:
            Dictionary with status and message
        """
        try:
            # Simple test query
            response = requests.post(
                f"{self.base_url}/places:searchText",
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

