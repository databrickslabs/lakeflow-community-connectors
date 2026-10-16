# ArcGIS Feature Service Connector for Databricks Lakeflow Connect.
#
# This connector enables data ingestion from ArcGIS Feature Services using the
# ArcGIS REST API. It supports both ArcGIS Online and ArcGIS Enterprise deployments.
#
# Supported features:
# - Dynamic schema discovery from Feature Service layers
# - Pagination using resultOffset and resultRecordCount
# - Incremental reads using OBJECTID or last_edited_date (if editor tracking enabled)
# - Token-based authentication
# - Geometry retrieval (optional)

import json
import requests
from datetime import datetime
from typing import Iterator, Any, Optional

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    BooleanType,
    ArrayType,
)


# Mapping from Esri field types to Spark data types
ESRI_TYPE_MAPPING = {
    "esriFieldTypeOID": LongType(),
    "esriFieldTypeGlobalID": StringType(),
    "esriFieldTypeString": StringType(),
    "esriFieldTypeInteger": LongType(),
    "esriFieldTypeSmallInteger": LongType(),
    "esriFieldTypeDouble": DoubleType(),
    "esriFieldTypeSingle": DoubleType(),
    "esriFieldTypeDate": LongType(),  # Epoch milliseconds
    "esriFieldTypeGeometry": StringType(),  # JSON representation
    "esriFieldTypeBlob": StringType(),  # Base64 encoded
    "esriFieldTypeRaster": StringType(),
    "esriFieldTypeGUID": StringType(),
    "esriFieldTypeXML": StringType(),
}


class LakeflowConnect:
    """
    ArcGIS Feature Service connector implementation for Lakeflow Connect.
    
    This connector reads feature data from ArcGIS Feature Services and
    supports incremental data ingestion based on OBJECTID or timestamps.
    """

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the ArcGIS connector with connection-level options.

        Expected options (PySpark custom data source):
            - service_url: Full URL to the Feature Service 
                          (e.g., https://services.arcgis.com/<org>/arcgis/rest/services/<name>/FeatureServer)
            - token (optional): Authentication token for secured services
        
        Expected options (Lakeflow Connect via UC HTTP connection):
            - host: Base host URL (e.g., https://sampleserver6.arcgisonline.com)
            - base_path: Path to the Feature Service (e.g., /arcgis/rest/services/Wildfire/FeatureServer)
            - bearer_token (optional): Authentication token for secured services
        
        Common options:
            - return_geometry (optional): Whether to include geometry in results (default: "true")
            - geometry_format (optional): Geometry output format - "geojson" or "esri" (default: "geojson")
            - out_sr (optional): Output spatial reference EPSG code (default: original CRS)
                                Set to "4326" for WGS84 lat/lon coordinates
            - max_record_count (optional): Override max records per request (default: 1000)
            - timeout (optional): Request timeout in seconds (default: 30)
        
        Note: service_url can be deferred to table_options for metadata reads where
        the URL is passed per-table via tableConfigs.
        """
        # Store options for later use (service_url may come from table_options)
        self._init_options = options
        
        # Try to get service_url from options (may be None for metadata reads)
        self._service_url: Optional[str] = self._resolve_service_url(options)
        
        # Get authentication token
        self._token = (
            options.get("token") or 
            options.get("bearer_token")
        )
        
        # Parse optional configuration
        self._return_geometry = options.get("return_geometry", "true").lower() == "true"
        
        # Geometry format: "geojson" (default) or "esri"
        geometry_format = options.get("geometry_format", "geojson").lower()
        self._use_geojson = geometry_format == "geojson"
        
        # Output spatial reference (EPSG code)
        self._out_sr = options.get("out_sr")
        if self._out_sr and self._out_sr.lower() == "native":
            self._out_sr = None
        
        try:
            self._max_record_count = int(options.get("max_record_count", 1000))
        except (TypeError, ValueError):
            self._max_record_count = 1000
        
        try:
            self._timeout = int(options.get("timeout", 30))
        except (TypeError, ValueError):
            self._timeout = 30

        # Configure a session for HTTP requests
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        })

        # Cache for layer metadata (populated on first access)
        self._layer_cache: dict[str, dict] = {}
        self._service_info: Optional[dict] = None

    def _resolve_service_url(self, options: dict[str, str]) -> Optional[str]:
        """Resolve service_url from options. Returns None if not found."""
        service_url = options.get("service_url")
        
        if not service_url:
            # Try alternate option names
            host = options.get("host")
            base_path = options.get("base_path", "")
            
            if host:
                # Construct service_url from host + base_path
                host = host.rstrip("/")
                if base_path:
                    service_url = host + "/" + base_path.lstrip("/")
                else:
                    service_url = host
        
        return service_url.rstrip("/") if service_url else None

    def _extract_table_config_from_options(
        self, table_name: str, options: dict[str, str]
    ) -> dict[str, str]:
        """
        Extract table-specific configuration from options.
        
        The upstream Lakeflow framework passes all table configurations as a JSON
        string in the 'tableConfigs' option during metadata reading. This method
        parses that JSON and extracts the config for the specific table.
        
        Args:
            table_name: The table name to extract config for
            options: The raw Spark options dictionary
            
        Returns:
            Merged options with table-specific config extracted from tableConfigs
        """
        merged = dict(options)
        
        # Check for tableConfigs (upstream framework metadata reading)
        table_configs_json = options.get("tableConfigs")
        if table_configs_json:
            try:
                all_table_configs = json.loads(table_configs_json)
                if isinstance(all_table_configs, dict):
                    # Get config for this specific table
                    table_config = all_table_configs.get(table_name, {})
                    if isinstance(table_config, dict):
                        # Merge table config into options (table config takes precedence)
                        merged.update(table_config)
            except (json.JSONDecodeError, TypeError):
                # If parsing fails, continue without tableConfigs
                pass
        
        return merged

    def _get_effective_config(self, table_options: dict[str, str]) -> tuple[str, Optional[str], bool, bool, Optional[str], int, int]:
        """
        Get effective configuration by merging init options with table options.
        Table options take precedence.
        
        Returns: (service_url, token, return_geometry, use_geojson, out_sr, max_record_count, timeout)
        """
        # Merge options - table_options override init options
        merged = {**self._init_options, **table_options}
        
        # Resolve service_url (required for API calls)
        service_url = self._resolve_service_url(merged)
        if not service_url:
                raise ValueError(
                    "ArcGIS connector requires 'service_url' in options. "
                    "For Lakeflow Connect pipelines, pass it in table_configuration: "
                    '{"table_configuration": {"service_url": "https://..."}}'
                )

        # Get other config values
        token = merged.get("token") or merged.get("bearer_token")
        return_geometry = merged.get("return_geometry", "true").lower() == "true"
        geometry_format = merged.get("geometry_format", "geojson").lower()
        use_geojson = geometry_format == "geojson"
        out_sr = merged.get("out_sr")
        if out_sr and out_sr.lower() == "native":
            out_sr = None
        
        try:
            max_record_count = int(merged.get("max_record_count", 1000))
        except (TypeError, ValueError):
            max_record_count = 1000
        
        try:
            timeout = int(merged.get("timeout", 30))
        except (TypeError, ValueError):
            timeout = 30
        
        return service_url, token, return_geometry, use_geojson, out_sr, max_record_count, timeout

    # Properties for backward compatibility
    @property
    def service_url(self) -> str:
        if self._service_url is None:
            raise ValueError("service_url not set. Pass it via table_options.")
        return self._service_url
    
    @property
    def token(self) -> Optional[str]:
        return self._token
    
    @property
    def return_geometry(self) -> bool:
        return self._return_geometry
    
    @property
    def use_geojson(self) -> bool:
        return self._use_geojson
    
    @property
    def out_sr(self) -> Optional[str]:
        return self._out_sr
    
    @property
    def max_record_count(self) -> int:
        return self._max_record_count
    
    @property
    def timeout(self) -> int:
        return self._timeout

    def list_tables(self, table_options: dict[str, str] = None) -> list[str]:
        """
        List names of all tables (layers) supported by this connector.

        Returns a list of layer names in the format "layer_<id>_<name>".
        
        Note: Requires service_url to be set either in init options or table_options.
        """
        service_info = self._get_service_info(table_options or {})
        
        tables = []
        
        # Add feature layers
        layers = service_info.get("layers", [])
        for layer in layers:
            layer_id = layer.get("id")
            layer_name = layer.get("name", f"layer_{layer_id}")
            # Sanitize layer name for use as a table identifier
            safe_name = self._sanitize_name(layer_name)
            tables.append(f"layer_{layer_id}_{safe_name}")
        
        # Add tables (non-spatial layers)
        tables_list = service_info.get("tables", [])
        for table in tables_list:
            table_id = table.get("id")
            table_name = table.get("name", f"table_{table_id}")
            safe_name = self._sanitize_name(table_name)
            tables.append(f"table_{table_id}_{safe_name}")

        return tables

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table (layer) from the Feature Service.

        The schema is dynamically retrieved from the layer metadata endpoint.
        """
        # Extract table-specific config from tableConfigs (upstream framework)
        effective_options = self._extract_table_config_from_options(table_name, table_options)
        
        # Get effective config to check return_geometry setting
        _, _, return_geometry, _, _, _, _ = self._get_effective_config(effective_options)
        
        layer_id = self._extract_layer_id(table_name)
        layer_info = self._get_layer_info(layer_id, effective_options)
        
        fields = []
        
        # Process each field from the layer metadata
        for field_info in layer_info.get("fields", []):
            field_name = field_info.get("name")
            esri_type = field_info.get("type")
            nullable = field_info.get("nullable", True)
            
            # Map Esri type to Spark type
            spark_type = ESRI_TYPE_MAPPING.get(esri_type, StringType())
            
            fields.append(StructField(field_name, spark_type, nullable))
        
        # Add geometry field if the layer has geometry
        if layer_info.get("geometryType") and return_geometry:
            # Check if there's already a geometry/Shape field
            existing_field_names = [f.name.lower() for f in fields]
            if "geometry" not in existing_field_names:
                fields.append(StructField("geometry", StringType(), True))

        return StructType(fields)

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch metadata for the given table (layer).

        Returns primary keys, cursor field, and ingestion type.
        """
        # Extract table-specific config from tableConfigs (upstream framework)
        effective_options = self._extract_table_config_from_options(table_name, table_options)
        
        layer_id = self._extract_layer_id(table_name)
        layer_info = self._get_layer_info(layer_id, effective_options)
        
        # Determine primary key field
        object_id_field = layer_info.get("objectIdField", "OBJECTID")
        global_id_field = layer_info.get("globalIdField")
        
        primary_keys = [object_id_field]
        if global_id_field:
            primary_keys.append(global_id_field)
        
        # Determine cursor field for incremental reads
        # Check if editor tracking fields exist
        cursor_field = None
        edit_fields = layer_info.get("editFieldsInfo", {})
        if edit_fields.get("editDateField"):
            cursor_field = edit_fields.get("editDateField")
        else:
            # Fall back to looking for common date fields
            for field in layer_info.get("fields", []):
                field_name = field.get("name", "").lower()
                if field_name in ("last_edited_date", "lasteditdate", "edit_date", "modified_date"):
                    cursor_field = field.get("name")
                    break
        
        # If no date cursor, we'll use OBJECTID for append-only pattern
        if cursor_field:
            return {
                "primary_keys": [object_id_field],
                "cursor_field": cursor_field,
                "ingestion_type": "cdc",
            }
        else:
            # Use append mode with OBJECTID as cursor
            return {
                "primary_keys": [object_id_field],
                "ingestion_type": "append",
            }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read records from a table (layer) and return raw JSON-like dictionaries.

        Supports pagination and incremental reads using OBJECTID or date fields.

        Required table_options:
            - service_url: Full URL to the Feature Service

        Optional table_options:
            - where: SQL where clause for filtering (default: "1=1")
            - out_fields: Comma-separated list of fields (default: "*")
            - max_pages_per_batch: Maximum pages to fetch per batch (default: 50)
            - start_date: Initial date for first run if no start_offset is provided (epoch ms)
            - lookback_ms: Lookback window in milliseconds (default: 60000 = 1 minute)
        """
        # Extract table-specific config from tableConfigs (upstream framework)
        effective_options = self._extract_table_config_from_options(table_name, table_options)
        
        layer_id = self._extract_layer_id(table_name)
        layer_info = self._get_layer_info(layer_id, effective_options)
        
        # Get configuration options
        where_clause = effective_options.get("where", "1=1")
        out_fields = effective_options.get("out_fields", "*")
        
        try:
            max_pages_per_batch = int(effective_options.get("max_pages_per_batch", 50))
        except (TypeError, ValueError):
            max_pages_per_batch = 50
        
        try:
            lookback_ms = int(effective_options.get("lookback_ms", 60000))
        except (TypeError, ValueError):
            lookback_ms = 60000
        
        # Determine the object ID field and cursor field
        object_id_field = layer_info.get("objectIdField", "OBJECTID")
        metadata = self.read_table_metadata(table_name, effective_options)
        cursor_field = metadata.get("cursor_field")
        ingestion_type = metadata.get("ingestion_type")
        
        # Build the query based on start_offset
        if ingestion_type == "cdc" and cursor_field:
            return self._read_with_date_cursor(
                layer_id,
                cursor_field,
                object_id_field,
                where_clause,
                out_fields,
                start_offset,
                max_pages_per_batch,
                lookback_ms,
                effective_options,
            )
        else:
            return self._read_with_objectid_cursor(
                layer_id,
                object_id_field,
                where_clause,
                out_fields,
                start_offset,
                max_pages_per_batch,
                effective_options,
            )

    def _read_with_date_cursor(
        self,
        layer_id: int,
        cursor_field: str,
        object_id_field: str,
        where_clause: str,
        out_fields: str,
        start_offset: dict,
        max_pages_per_batch: int,
        lookback_ms: int,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read features using a date-based cursor for CDC."""
        # Get effective config
        _, _, return_geometry, _, _, max_record_count, _ = self._get_effective_config(table_options)
        
        # Determine the starting cursor value
        cursor_value = None
        if start_offset and isinstance(start_offset, dict):
            cursor_value = start_offset.get("cursor")
        if cursor_value is None:
            cursor_value = table_options.get("start_date")
        
        # Build the where clause with cursor
        full_where = where_clause
        if cursor_value is not None:
            try:
                cursor_int = int(cursor_value)
                full_where = f"({where_clause}) AND {cursor_field} >= {cursor_int}"
            except (TypeError, ValueError):
                pass  # Invalid cursor, use base where clause
        
        records: list[dict[str, Any]] = []
        max_cursor_value: Optional[int] = None
        
        result_offset = 0
        pages_fetched = 0
        has_more = True
        
        while has_more and pages_fetched < max_pages_per_batch:
            response = self._query_layer(
                layer_id,
                table_options,
                where=full_where,
                out_fields=out_fields,
                order_by=f"{cursor_field} ASC, {object_id_field} ASC",
                result_offset=result_offset,
                result_record_count=max_record_count,
            )
            
            features = response.get("features", [])
            if not features:
                break
            
            for feature in features:
                record = self._feature_to_record(feature, return_geometry)
                records.append(record)
                
                # Track the maximum cursor value
                cursor_val = record.get(cursor_field)
                if cursor_val is not None and isinstance(cursor_val, (int, float)):
                    cursor_int = int(cursor_val)
                    if max_cursor_value is None or cursor_int > max_cursor_value:
                        max_cursor_value = cursor_int
            
            # Check if there are more records
            exceeded_limit = response.get("exceededTransferLimit", False)
            has_more = exceeded_limit or len(features) == max_record_count
            
            result_offset += len(features)
            pages_fetched += 1
        
        # Compute the next cursor with lookback
        next_cursor = cursor_value
        if max_cursor_value is not None:
            # Apply lookback window
            next_cursor = str(max_cursor_value - lookback_ms)
        
        # If no new records, return the same offset
        if not records and start_offset:
            next_offset = start_offset
        else:
            next_offset = {"cursor": next_cursor} if next_cursor else {}
        
        return iter(records), next_offset

    def _read_with_objectid_cursor(
        self,
        layer_id: int,
        object_id_field: str,
        where_clause: str,
        out_fields: str,
        start_offset: dict,
        max_pages_per_batch: int,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read features using an OBJECTID-based cursor for append-only."""
        # Get effective config
        _, _, return_geometry, _, _, max_record_count, _ = self._get_effective_config(table_options)
        
        # Determine the starting cursor value
        cursor_value = None
        if start_offset and isinstance(start_offset, dict):
            cursor_value = start_offset.get("cursor")
        if cursor_value is None:
            cursor_value = table_options.get("start_objectid", "0")
        
        # Build the where clause with cursor
        full_where = where_clause
        if cursor_value is not None:
            try:
                cursor_int = int(cursor_value)
                if cursor_int > 0:
                    full_where = f"({where_clause}) AND {object_id_field} > {cursor_int}"
            except (TypeError, ValueError):
                pass
        
        records: list[dict[str, Any]] = []
        max_object_id: Optional[int] = None
        
        result_offset = 0
        pages_fetched = 0
        has_more = True
        
        while has_more and pages_fetched < max_pages_per_batch:
            response = self._query_layer(
                layer_id,
                table_options,
                where=full_where,
                out_fields=out_fields,
                order_by=f"{object_id_field} ASC",
                result_offset=result_offset,
                result_record_count=max_record_count,
            )
            
            features = response.get("features", [])
            if not features:
                break
            
            for feature in features:
                record = self._feature_to_record(feature, return_geometry)
                records.append(record)
                
                # Track the maximum object ID
                oid = record.get(object_id_field)
                if oid is not None and isinstance(oid, (int, float)):
                    oid_int = int(oid)
                    if max_object_id is None or oid_int > max_object_id:
                        max_object_id = oid_int
            
            # Check if there are more records
            exceeded_limit = response.get("exceededTransferLimit", False)
            has_more = exceeded_limit or len(features) == max_record_count
            
            result_offset += len(features)
            pages_fetched += 1
        
        # Compute the next cursor
        next_cursor = cursor_value
        if max_object_id is not None:
            next_cursor = str(max_object_id)
        
        # If no new records, return the same offset
        if not records and start_offset:
            next_offset = start_offset
        else:
            next_offset = {"cursor": next_cursor} if next_cursor else {}
        
        return iter(records), next_offset

    def _query_layer(
        self,
        layer_id: int,
        table_options: dict[str, str],
        where: str = "1=1",
        out_fields: str = "*",
        order_by: Optional[str] = None,
        result_offset: int = 0,
        result_record_count: int = 1000,
    ) -> dict:
        """Execute a query against a Feature Service layer."""
        # Get effective config from merged options
        service_url, token, return_geometry, use_geojson, out_sr, max_record_count, timeout = \
            self._get_effective_config(table_options)
        
        url = f"{service_url}/{layer_id}/query"
        
        # Use GeoJSON format for standard geometry output
        output_format = "geojson" if use_geojson else "json"
        
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": str(return_geometry).lower(),
            "resultOffset": result_offset,
            "resultRecordCount": result_record_count,
            "f": output_format,
        }
        
        # Add output spatial reference if specified
        # This transforms coordinates to the requested CRS (e.g., 4326 for WGS84)
        if out_sr:
            params["outSR"] = out_sr
        
        if order_by:
            params["orderByFields"] = order_by
        
        if token:
            params["token"] = token
        
        response = self._session.get(url, params=params, timeout=timeout)
        
        if response.status_code != 200:
            raise RuntimeError(
                f"ArcGIS API error for layer {layer_id}: "
                f"{response.status_code} {response.text}"
            )
        
        result = response.json()
        
        # Check for ArcGIS-specific errors (only in Esri JSON format)
        if "error" in result:
            error = result["error"]
            raise RuntimeError(
                f"ArcGIS query error: {error.get('code')} - {error.get('message')}"
            )
        
        return result

    def _feature_to_record(self, feature: dict, return_geometry: bool) -> dict[str, Any]:
        """Convert an ArcGIS feature to a flat record dictionary.
        
        Handles both GeoJSON and Esri JSON formats:
        - GeoJSON: attributes in 'properties', geometry in GeoJSON format
        - Esri JSON: attributes in 'attributes', geometry in Esri format
        """
        import json
        
        # GeoJSON format has 'properties', Esri JSON has 'attributes'
        if "properties" in feature:
            # GeoJSON format
            record = dict(feature.get("properties", {}))
        else:
            # Esri JSON format
            record = dict(feature.get("attributes", {}))
        
        # Add geometry if present and requested
        if return_geometry and "geometry" in feature:
            geom = feature["geometry"]
            if geom is not None:
                record["geometry"] = json.dumps(geom)
            else:
                record["geometry"] = None
        
        return record

    def _get_service_info(self, table_options: dict[str, str] = None) -> dict:
        """Fetch and cache the Feature Service metadata."""
        # Get effective config from merged options
        table_options = table_options or {}
        service_url, token, _, _, _, _, timeout = self._get_effective_config(table_options)
        
        # Use service_url as cache key since different table_options could have different URLs
        cache_key = service_url
        if cache_key not in self._layer_cache.get("_service_info_cache", {}):
            url = service_url
            params = {"f": "json"}
            
            if token:
                params["token"] = token
            
            response = self._session.get(url, params=params, timeout=timeout)
            
            if response.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch Feature Service info: "
                    f"{response.status_code} {response.text}"
                )
            
            result = response.json()
            
            if "error" in result:
                error = result["error"]
                raise RuntimeError(
                    f"ArcGIS service error: {error.get('code')} - {error.get('message')}"
                )
            
            # Cache by service_url
            if "_service_info_cache" not in self._layer_cache:
                self._layer_cache["_service_info_cache"] = {}
            self._layer_cache["_service_info_cache"][cache_key] = result
        
        return self._layer_cache["_service_info_cache"][cache_key]

    def _get_layer_info(self, layer_id: int, table_options: dict[str, str] = None) -> dict:
        """Fetch and cache layer metadata."""
        # Get effective config from merged options
        table_options = table_options or {}
        service_url, token, _, _, _, _, timeout = self._get_effective_config(table_options)
        
        cache_key = f"{service_url}_{layer_id}"
        
        if cache_key not in self._layer_cache:
            url = f"{service_url}/{layer_id}"
            params = {"f": "json"}
            
            if token:
                params["token"] = token
            
            response = self._session.get(url, params=params, timeout=timeout)
            
            if response.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch layer info for layer {layer_id}: "
                    f"{response.status_code} {response.text}"
                )
            
            result = response.json()
            
            if "error" in result:
                error = result["error"]
                raise RuntimeError(
                    f"ArcGIS layer error: {error.get('code')} - {error.get('message')}"
                )
            
            self._layer_cache[cache_key] = result
        
        return self._layer_cache[cache_key]

    def _extract_layer_id(self, table_name: str) -> int:
        """Extract the layer ID from a table name."""
        # Table name format: "layer_<id>_<name>" or "table_<id>_<name>"
        parts = table_name.split("_", 2)
        if len(parts) >= 2:
            try:
                return int(parts[1])
            except ValueError:
                pass
        
        raise ValueError(f"Could not extract layer ID from table name: {table_name!r}")

    @staticmethod
    def _sanitize_name(name: str) -> str:
        """Sanitize a name for use as a table identifier."""
        # Replace spaces and special characters with underscores
        import re
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        # Remove consecutive underscores
        sanitized = re.sub(r"_+", "_", sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip("_")
        return sanitized.lower() if sanitized else "unnamed"

