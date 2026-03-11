# ArcGIS Feature Service Connector

A Databricks Lakeflow Connect connector for ingesting data from ArcGIS Feature Services.

## Overview

This connector enables data ingestion from ArcGIS Feature Services into Databricks Lakehouse. It supports:

- **ArcGIS Online**: Cloud-hosted Feature Services
- **ArcGIS Enterprise**: Self-hosted ArcGIS Server deployments
- **Dynamic schema discovery**: Automatically discovers layers and their schemas
- **Incremental reads**: Supports CDC using `last_edited_date` or append-only using `OBJECTID`
- **Pagination**: Handles large datasets with built-in pagination
- **Geometry support**: Optionally includes spatial geometry data (GeoJSON or Esri format)

## Supported Tables

The connector dynamically discovers available tables (layers) from the Feature Service:

| Table Pattern | Description |
|--------------|-------------|
| `layer_<id>_<name>` | Feature layers with spatial geometry |
| `table_<id>_<name>` | Non-spatial tables |

## Configuration

### Pipeline Spec Configuration

Configuration is passed via `table_configuration` for each table in the pipeline spec:

```python
pipeline_spec = {
    "connection_name": "arcgis_placeholder",
    "objects": [
        {
            "table": {
                "source_table": "layer_0_wildfire_response_points",
                "destination_catalog": "your_catalog",
                "destination_schema": "arcgis_data",
                "table_configuration": {
                    "service_url": "https://services.arcgis.com/<org>/arcgis/rest/services/<name>/FeatureServer",
                    "token": "YOUR_TOKEN",  # Optional
                    "geometry_format": "geojson",
                }
            }
        }
    ]
}
```

### Connection Settings

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `service_url` | Yes | - | Full URL to the Feature Service |
| `token` | No | - | Authentication token for secured services |
| `return_geometry` | No | `true` | Whether to include geometry in results |
| `geometry_format` | No | `geojson` | Output format: `geojson` or `esri` |
| `out_sr` | No | - | Output spatial reference EPSG code (e.g., `4326` for WGS84) |
| `max_record_count` | No | `1000` | Maximum records per request |
| `timeout` | No | `30` | Request timeout in seconds |

### Table Options

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `where` | No | `1=1` | SQL where clause for filtering features |
| `out_fields` | No | `*` | Comma-separated list of fields to return |
| `max_pages_per_batch` | No | `50` | Maximum pages to fetch per batch |
| `start_date` | No | - | Initial cursor date for first run (epoch ms) |
| `start_objectid` | No | `0` | Initial OBJECTID for append-only mode |
| `lookback_ms` | No | `60000` | Lookback window in milliseconds for CDC |

## Authentication

### Public Services

For public Feature Services, no authentication is required:

```python
"table_configuration": {
    "service_url": "https://sampleserver6.arcgisonline.com/arcgis/rest/services/Wildfire/FeatureServer"
}
```

### Token Authentication

For secured services, generate a token and include it:

```python
"table_configuration": {
    "service_url": "https://services.arcgis.com/<org>/arcgis/rest/services/<name>/FeatureServer",
    "token": "YOUR_ACCESS_TOKEN"
}
```

**Generating a Token:**

```bash
curl -X POST \
  "https://www.arcgis.com/sharing/rest/generateToken" \
  -d "username=YOUR_USERNAME" \
  -d "password=YOUR_PASSWORD" \
  -d "client=referer" \
  -d "referer=https://your-app.com" \
  -d "expiration=120" \
  -d "f=json"
```

### API Key Authentication

For production use, generate an API key from the [ArcGIS Developer Dashboard](https://developers.arcgis.com/) and use it as the token.

## Ingestion Types

The connector supports two ingestion modes based on whether the Feature Service has editor tracking enabled:

### CDC (Change Data Capture)

Used when the Feature Service has editor tracking enabled. The connector tracks changes using the `last_edited_date` field.

**Metadata returned:**
```json
{
    "primary_keys": ["OBJECTID"],
    "cursor_field": "last_edited_date",
    "ingestion_type": "cdc"
}
```

### Append-Only

Used when editor tracking is not available. The connector tracks new features using `OBJECTID`.

**Metadata returned:**
```json
{
    "primary_keys": ["OBJECTID"],
    "ingestion_type": "append"
}
```

## Schema Mapping

| ArcGIS Type | Spark Type | Notes |
|-------------|------------|-------|
| `esriFieldTypeOID` | `LongType` | Primary key |
| `esriFieldTypeGlobalID` | `StringType` | GUID format |
| `esriFieldTypeString` | `StringType` | Text fields |
| `esriFieldTypeInteger` | `LongType` | Integer fields |
| `esriFieldTypeSmallInteger` | `LongType` | Small integer fields |
| `esriFieldTypeDouble` | `DoubleType` | Double precision |
| `esriFieldTypeSingle` | `DoubleType` | Single precision |
| `esriFieldTypeDate` | `LongType` | Epoch milliseconds |
| `esriFieldTypeGeometry` | `StringType` | JSON geometry |

## Example Pipeline Spec

### Basic Configuration

```python
pipeline_spec = {
    "connection_name": "arcgis_placeholder",
    "objects": [
        {
            "table": {
                "source_table": "layer_0_points",
                "destination_catalog": "main",
                "destination_schema": "arcgis_data",
                "table_configuration": {
                    "service_url": "https://sampleserver6.arcgisonline.com/arcgis/rest/services/Wildfire/FeatureServer"
                }
            }
        }
    ]
}
```

### With Authentication and Options

```python
pipeline_spec = {
    "connection_name": "arcgis_production",
    "objects": [
        {
            "table": {
                "source_table": "layer_0_assets",
                "destination_catalog": "production",
                "destination_schema": "gis_data",
                "destination_table": "asset_locations",
                "table_configuration": {
                    "service_url": "https://services.arcgis.com/abc123/arcgis/rest/services/Assets/FeatureServer",
                    "token": "YOUR_TOKEN",
                    "geometry_format": "geojson",
                    "return_geometry": "true",
                    "where": "Status = 'Active'",
                    "scd_type": "SCD_TYPE_2"
                }
            }
        },
        {
            "table": {
                "source_table": "layer_1_zones",
                "destination_catalog": "production",
                "destination_schema": "gis_data",
                "table_configuration": {
                    "service_url": "https://services.arcgis.com/abc123/arcgis/rest/services/Assets/FeatureServer",
                    "token": "YOUR_TOKEN",
                    "geometry_format": "esri"
                }
            }
        }
    ]
}
```

## Testing

### Running Tests

1. Update the configuration files:
   - `sources/arcgis/configs/dev_config.json` - Connection settings
   - `sources/arcgis/configs/dev_table_config.json` - Table-specific options

2. Run the test suite:

```bash
pytest sources/arcgis/test/test_arcgis_lakeflow_connect.py -v
```

### Using a Public Sample Service

For testing without authentication, use Esri's public sample services:

```python
"table_configuration": {
    "service_url": "https://sampleserver6.arcgisonline.com/arcgis/rest/services/Wildfire/FeatureServer"
}
```

## Limitations

- **Max Record Count**: Limited by the server's `maxRecordCount` setting (typically 1000-2000)
- **Geometry Size**: Large geometries may impact performance
- **Editor Tracking**: CDC mode requires editor tracking to be enabled on the Feature Service
- **Attachments**: Feature attachments are not currently supported
- **Related Tables**: Relationships between tables are not automatically followed

## Troubleshooting

### Common Errors

**Token Expired:**
```
ArcGIS query error: 498 - Invalid token
```
Solution: Generate a new token.

**No Features Returned:**
```
features: []
```
Solution: Check your `where` clause and ensure the service contains data.

**Rate Limiting:**
```
ArcGIS API error: 429
```
Solution: Reduce `max_pages_per_batch` or add delays between requests.

**Service URL Not Found:**
```
ArcGIS connector requires 'service_url' in options
```
Solution: Ensure `service_url` is specified in `table_configuration` for each table.

## References

- [ArcGIS REST API Documentation](https://developers.arcgis.com/rest/services-reference/enterprise/feature-service/)
- [ArcGIS Feature Service Query](https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/)
- [Lakeflow Community Connectors](https://github.com/databrickslabs/lakeflow-community-connectors)
