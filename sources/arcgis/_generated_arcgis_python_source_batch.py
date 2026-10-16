# ==============================================================================
# Merged Lakeflow Source: arcgis (Batch-Only Version)
# ==============================================================================
# This file is for DBR versions that don't support SimpleDataSourceStreamReader
# Use this for DBR < 15.0 or clusters without streaming data source support
# ==============================================================================

from datetime import datetime
from decimal import Decimal
from typing import Any, Iterator, Optional

from pyspark.sql import Row
from pyspark.sql.types import *

import requests
import re
import json


# Mapping from Esri field types to Spark data types
ESRI_TYPE_MAPPING = {
    "esriFieldTypeOID": LongType(),
    "esriFieldTypeGlobalID": StringType(),
    "esriFieldTypeString": StringType(),
    "esriFieldTypeInteger": LongType(),
    "esriFieldTypeSmallInteger": LongType(),
    "esriFieldTypeDouble": DoubleType(),
    "esriFieldTypeSingle": DoubleType(),
    "esriFieldTypeDate": LongType(),
    "esriFieldTypeGeometry": StringType(),
    "esriFieldTypeBlob": StringType(),
    "esriFieldTypeRaster": StringType(),
    "esriFieldTypeGUID": StringType(),
    "esriFieldTypeXML": StringType(),
}


class ArcGISConnector:
    """
    ArcGIS Feature Service connector for reading data into Spark DataFrames.
    
    This is a batch-only connector that works on all DBR versions.
    """

    def __init__(self, service_url: str, token: str = None, return_geometry: bool = True, 
                 max_record_count: int = 1000, timeout: int = 30):
        """
        Initialize the ArcGIS connector.

        Args:
            service_url: Full URL to the Feature Service
            token: Authentication token for secured services (optional)
            return_geometry: Whether to include geometry in results (default: True)
            max_record_count: Max records per request (default: 1000)
            timeout: Request timeout in seconds (default: 30)
        """
        self.service_url = service_url.rstrip("/")
        self.token = token
        self.return_geometry = return_geometry
        self.max_record_count = max_record_count
        self.timeout = timeout
        
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        })
        
        self._layer_cache = {}
        self._service_info = None

    def list_layers(self) -> list:
        """List all layers in the Feature Service."""
        service_info = self._get_service_info()
        
        layers = []
        for layer in service_info.get("layers", []):
            layers.append({
                "id": layer.get("id"),
                "name": layer.get("name"),
                "type": "layer"
            })
        
        for table in service_info.get("tables", []):
            layers.append({
                "id": table.get("id"),
                "name": table.get("name"),
                "type": "table"
            })
        
        return layers

    def get_layer_schema(self, layer_id: int) -> StructType:
        """Get the Spark schema for a layer."""
        layer_info = self._get_layer_info(layer_id)
        
        fields = []
        for field_info in layer_info.get("fields", []):
            field_name = field_info.get("name")
            esri_type = field_info.get("type")
            nullable = field_info.get("nullable", True)
            
            spark_type = ESRI_TYPE_MAPPING.get(esri_type, StringType())
            fields.append(StructField(field_name, spark_type, nullable))
        
        if layer_info.get("geometryType") and self.return_geometry:
            existing_names = [f.name.lower() for f in fields]
            if "geometry" not in existing_names:
                fields.append(StructField("geometry", StringType(), True))
        
        return StructType(fields)

    def read_layer(self, layer_id: int, where: str = "1=1", out_fields: str = "*",
                   max_records: int = None) -> list:
        """
        Read all features from a layer.
        
        Args:
            layer_id: The layer ID to read
            where: SQL where clause for filtering
            out_fields: Comma-separated list of fields to return
            max_records: Maximum total records to return (None for all)
            
        Returns:
            List of feature dictionaries
        """
        all_records = []
        result_offset = 0
        
        while True:
            response = self._query_layer(
                layer_id,
                where=where,
                out_fields=out_fields,
                result_offset=result_offset,
                result_record_count=self.max_record_count,
            )
            
            features = response.get("features", [])
            if not features:
                break
            
            for feature in features:
                record = dict(feature.get("attributes", {}))
                if self.return_geometry and "geometry" in feature:
                    record["geometry"] = json.dumps(feature["geometry"])
                all_records.append(record)
            
            if max_records and len(all_records) >= max_records:
                all_records = all_records[:max_records]
                break
            
            exceeded_limit = response.get("exceededTransferLimit", False)
            if not exceeded_limit and len(features) < self.max_record_count:
                break
            
            result_offset += len(features)
        
        return all_records

    def read_layer_as_df(self, spark, layer_id: int, where: str = "1=1", 
                         out_fields: str = "*", max_records: int = None):
        """
        Read a layer directly into a Spark DataFrame.
        
        Args:
            spark: SparkSession
            layer_id: The layer ID to read
            where: SQL where clause for filtering
            out_fields: Comma-separated list of fields to return
            max_records: Maximum total records to return
            
        Returns:
            Spark DataFrame
        """
        schema = self.get_layer_schema(layer_id)
        records = self.read_layer(layer_id, where, out_fields, max_records)
        
        if not records:
            return spark.createDataFrame([], schema)
        
        # Convert records to Rows
        rows = []
        for record in records:
            row_dict = {}
            for field in schema.fields:
                value = record.get(field.name)
                row_dict[field.name] = self._convert_value(value, field.dataType)
            rows.append(Row(**row_dict))
        
        return spark.createDataFrame(rows, schema)

    def _convert_value(self, value: Any, field_type: DataType) -> Any:
        """Convert a value to the appropriate Spark type."""
        if value is None:
            return None
        
        if isinstance(field_type, StringType):
            return str(value) if value is not None else None
        elif isinstance(field_type, (LongType, IntegerType)):
            return int(value) if value is not None else None
        elif isinstance(field_type, DoubleType):
            return float(value) if value is not None else None
        elif isinstance(field_type, BooleanType):
            return bool(value)
        else:
            return value

    def _query_layer(self, layer_id: int, where: str = "1=1", out_fields: str = "*",
                     order_by: str = None, result_offset: int = 0, 
                     result_record_count: int = 1000) -> dict:
        """Execute a query against a Feature Service layer."""
        url = f"{self.service_url}/{layer_id}/query"
        
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": str(self.return_geometry).lower(),
            "resultOffset": result_offset,
            "resultRecordCount": result_record_count,
            "f": "json",
        }
        
        if order_by:
            params["orderByFields"] = order_by
        
        if self.token:
            params["token"] = self.token
        
        response = self._session.get(url, params=params, timeout=self.timeout)
        
        if response.status_code != 200:
            raise RuntimeError(f"ArcGIS API error: {response.status_code} {response.text}")
        
        result = response.json()
        
        if "error" in result:
            error = result["error"]
            raise RuntimeError(f"ArcGIS query error: {error.get('code')} - {error.get('message')}")
        
        return result

    def _get_service_info(self) -> dict:
        """Fetch and cache the Feature Service metadata."""
        if self._service_info is None:
            params = {"f": "json"}
            if self.token:
                params["token"] = self.token
            
            response = self._session.get(self.service_url, params=params, timeout=self.timeout)
            
            if response.status_code != 200:
                raise RuntimeError(f"Failed to fetch service info: {response.status_code}")
            
            result = response.json()
            if "error" in result:
                raise RuntimeError(f"ArcGIS error: {result['error']}")
            
            self._service_info = result
        
        return self._service_info

    def _get_layer_info(self, layer_id: int) -> dict:
        """Fetch and cache layer metadata."""
        cache_key = str(layer_id)
        
        if cache_key not in self._layer_cache:
            url = f"{self.service_url}/{layer_id}"
            params = {"f": "json"}
            if self.token:
                params["token"] = self.token
            
            response = self._session.get(url, params=params, timeout=self.timeout)
            
            if response.status_code != 200:
                raise RuntimeError(f"Failed to fetch layer info: {response.status_code}")
            
            result = response.json()
            if "error" in result:
                raise RuntimeError(f"ArcGIS error: {result['error']}")
            
            self._layer_cache[cache_key] = result
        
        return self._layer_cache[cache_key]


# Convenience function for quick access
def read_arcgis_layer(spark, service_url: str, layer_id: int, 
                      token: str = None, where: str = "1=1",
                      return_geometry: bool = True, max_records: int = None):
    """
    Quick function to read an ArcGIS Feature Service layer into a DataFrame.
    
    Args:
        spark: SparkSession
        service_url: Full URL to the Feature Service
        layer_id: Layer ID to read (usually 0)
        token: Authentication token (optional)
        where: SQL where clause for filtering
        return_geometry: Include geometry in results
        max_records: Maximum records to return
        
    Returns:
        Spark DataFrame
        
    Example:
        df = read_arcgis_layer(
            spark,
            "https://services.arcgis.com/org/arcgis/rest/services/MyService/FeatureServer",
            layer_id=0,
            where="Status = 'Active'"
        )
    """
    connector = ArcGISConnector(
        service_url=service_url,
        token=token,
        return_geometry=return_geometry
    )
    return connector.read_layer_as_df(spark, layer_id, where=where, max_records=max_records)

