"""Static schema definitions for OSI PI connector tables.

This module contains all Spark StructType schema definitions and table metadata
for the OSI PI Lakeflow connector. These are derived from the PI Web API
documentation.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.sources.osipi.osipi_constants import (
    TABLE_AF_HIERARCHY,
    TABLE_AF_TABLE_ROWS,
    TABLE_AF_TABLES,
    TABLE_ANALYSES,
    TABLE_ANALYSIS_TEMPLATES,
    TABLE_ASSET_DATABASES,
    TABLE_ASSET_SERVERS,
    TABLE_ATTRIBUTE_TEMPLATES,
    TABLE_CALCULATED,
    TABLE_CATEGORIES,
    TABLE_CURRENT_VALUE,
    TABLE_DATASERVERS,
    TABLE_ELEMENT_ATTRIBUTES,
    TABLE_ELEMENT_TEMPLATE_ATTRIBUTES,
    TABLE_ELEMENT_TEMPLATES,
    TABLE_END,
    TABLE_EVENT_FRAMES,
    TABLE_EVENTFRAME_ACKS,
    TABLE_EVENTFRAME_ANNOTATIONS,
    TABLE_EVENTFRAME_ATTRIBUTES,
    TABLE_EVENTFRAME_REFERENCED_ELEMENTS,
    TABLE_EVENTFRAME_TEMPLATE_ATTRIBUTES,
    TABLE_EVENTFRAME_TEMPLATES,
    TABLE_INTERPOLATED,
    TABLE_LINKS,
    TABLE_PLOT,
    TABLE_POINT_ATTRIBUTES,
    TABLE_POINT_TYPE_CATALOG,
    TABLE_POINTS,
    TABLE_RECORDED_AT_TIME,
    TABLE_STREAMSET_END,
    TABLE_STREAMSET_INTERPOLATED,
    TABLE_STREAMSET_PLOT,
    TABLE_STREAMSET_RECORDED,
    TABLE_STREAMSET_SUMMARY,
    TABLE_SUMMARY,
    TABLE_TIMESERIES,
    TABLE_UNITS_OF_MEASURE,
    TABLE_VALUE_AT_TIME,
)


# =============================================================================
# Reusable Schema Components
# =============================================================================

TS_VALUE_SCHEMA = StructType(
    [
        StructField("tag_webid", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("value", DoubleType(), True),
        StructField("good", BooleanType(), True),
        StructField("questionable", BooleanType(), True),
        StructField("substituted", BooleanType(), True),
        StructField("annotated", BooleanType(), True),
        StructField("units", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)
"""Common schema for time-series value tables."""


# =============================================================================
# Discovery & Inventory Schemas
# =============================================================================

DATASERVERS_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
    ]
)

POINTS_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("descriptor", StringType(), True),
        StructField("engineering_units", StringType(), True),
        StructField("path", StringType(), True),
        StructField("dataserver_webid", StringType(), True),
    ]
)

POINT_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("point_webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("value", StringType(), True),
        StructField("type", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

POINT_TYPE_CATALOG_SCHEMA = StructType(
    [
        StructField("point_type", StringType(), False),
        StructField("engineering_units", StringType(), True),
        StructField("count_points", LongType(), True),
    ]
)


# =============================================================================
# Time Series Schemas
# =============================================================================

END_SCHEMA = StructType(
    [
        StructField("tag_webid", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("good", BooleanType(), True),
        StructField("questionable", BooleanType(), True),
        StructField("substituted", BooleanType(), True),
        StructField("annotated", BooleanType(), True),
        StructField("units", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

CURRENT_VALUE_SCHEMA = StructType(
    [
        StructField("tag_webid", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("good", BooleanType(), True),
        StructField("questionable", BooleanType(), True),
        StructField("substituted", BooleanType(), True),
        StructField("annotated", BooleanType(), True),
        StructField("units", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

VALUE_AT_TIME_SCHEMA = StructType(
    [
        StructField("tag_webid", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("good", BooleanType(), True),
        StructField("questionable", BooleanType(), True),
        StructField("substituted", BooleanType(), True),
        StructField("annotated", BooleanType(), True),
        StructField("units", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

RECORDED_AT_TIME_SCHEMA = StructType(
    [
        StructField("tag_webid", StringType(), False),
        StructField("query_time", TimestampType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("good", BooleanType(), True),
        StructField("questionable", BooleanType(), True),
        StructField("substituted", BooleanType(), True),
        StructField("annotated", BooleanType(), True),
        StructField("units", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

SUMMARY_SCHEMA = StructType(
    [
        StructField("tag_webid", StringType(), False),
        StructField("summary_type", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("good", BooleanType(), True),
        StructField("questionable", BooleanType(), True),
        StructField("substituted", BooleanType(), True),
        StructField("annotated", BooleanType(), True),
        StructField("units", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

STREAMSET_SUMMARY_SCHEMA = StructType(
    [
        StructField("tag_webid", StringType(), False),
        StructField("summary_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("value", DoubleType(), True),
        StructField("good", BooleanType(), True),
        StructField("questionable", BooleanType(), True),
        StructField("substituted", BooleanType(), True),
        StructField("annotated", BooleanType(), True),
        StructField("units", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

CALCULATED_SCHEMA = StructType(
    [
        StructField("tag_webid", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("value", DoubleType(), True),
        StructField("units", StringType(), True),
        StructField("calculation_type", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)


# =============================================================================
# Asset Framework Schemas
# =============================================================================

ASSET_SERVERS_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("path", StringType(), True),
    ]
)

ASSET_DATABASES_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("path", StringType(), True),
        StructField("assetserver_webid", StringType(), True),
    ]
)

AF_HIERARCHY_SCHEMA = StructType(
    [
        StructField("element_webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("template_name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("path", StringType(), True),
        StructField("parent_webid", StringType(), True),
        StructField("depth", LongType(), True),
        StructField("category_names", ArrayType(StringType()), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

ELEMENT_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("element_webid", StringType(), False),
        StructField("attribute_webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("path", StringType(), True),
        StructField("type", StringType(), True),
        StructField("default_units_name", StringType(), True),
        StructField("data_reference_plugin", StringType(), True),
        StructField("is_configuration_item", BooleanType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

ELEMENT_TEMPLATES_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("path", StringType(), True),
        StructField("assetdatabase_webid", StringType(), True),
    ]
)

ELEMENT_TEMPLATE_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("path", StringType(), True),
        StructField("type", StringType(), True),
        StructField("default_units_name", StringType(), True),
        StructField("data_reference_plugin", StringType(), True),
        StructField("is_configuration_item", BooleanType(), True),
        StructField("elementtemplate_webid", StringType(), True),
        StructField("assetdatabase_webid", StringType(), True),
    ]
)

CATEGORIES_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("category_type", StringType(), True),
        StructField("assetdatabase_webid", StringType(), True),
    ]
)

ATTRIBUTE_TEMPLATES_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("path", StringType(), True),
        StructField("type", StringType(), True),
        StructField("elementtemplate_webid", StringType(), True),
        StructField("assetdatabase_webid", StringType(), True),
    ]
)

ANALYSES_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("path", StringType(), True),
        StructField("analysis_template_name", StringType(), True),
        StructField("target_element_webid", StringType(), True),
        StructField("assetdatabase_webid", StringType(), True),
    ]
)

ANALYSIS_TEMPLATES_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("path", StringType(), True),
        StructField("assetdatabase_webid", StringType(), True),
    ]
)

AF_TABLES_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("path", StringType(), True),
        StructField("assetdatabase_webid", StringType(), True),
    ]
)

AF_TABLE_ROWS_SCHEMA = StructType(
    [
        StructField("table_webid", StringType(), False),
        StructField("row_index", LongType(), True),
        StructField("columns", MapType(StringType(), StringType()), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

UNITS_OF_MEASURE_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("abbreviation", StringType(), True),
        StructField("quantity_type", StringType(), True),
    ]
)


# =============================================================================
# Event Frame Schemas
# =============================================================================

EVENT_FRAMES_SCHEMA = StructType(
    [
        StructField("event_frame_webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("template_name", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("primary_referenced_element_webid", StringType(), True),
        StructField("description", StringType(), True),
        StructField("category_names", ArrayType(StringType()), True),
        StructField("attributes", MapType(StringType(), StringType()), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

EVENTFRAME_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("event_frame_webid", StringType(), False),
        StructField("attribute_webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("path", StringType(), True),
        StructField("type", StringType(), True),
        StructField("default_units_name", StringType(), True),
        StructField("data_reference_plugin", StringType(), True),
        StructField("is_configuration_item", BooleanType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

EVENTFRAME_TEMPLATES_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("path", StringType(), True),
        StructField("assetdatabase_webid", StringType(), True),
    ]
)

EVENTFRAME_TEMPLATE_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("webid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("path", StringType(), True),
        StructField("type", StringType(), True),
        StructField("eventframe_template_webid", StringType(), True),
        StructField("assetdatabase_webid", StringType(), True),
    ]
)

EVENTFRAME_REFERENCED_ELEMENTS_SCHEMA = StructType(
    [
        StructField("event_frame_webid", StringType(), False),
        StructField("element_webid", StringType(), False),
        StructField("relationship_type", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

EVENTFRAME_ACKS_SCHEMA = StructType(
    [
        StructField("event_frame_webid", StringType(), False),
        StructField("ack_id", StringType(), False),
        StructField("ack_timestamp", TimestampType(), True),
        StructField("ack_user", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)

EVENTFRAME_ANNOTATIONS_SCHEMA = StructType(
    [
        StructField("event_frame_webid", StringType(), False),
        StructField("annotation_id", StringType(), False),
        StructField("annotation_timestamp", TimestampType(), True),
        StructField("annotation_user", StringType(), True),
        StructField("text", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)


# =============================================================================
# Governance & Diagnostics Schemas
# =============================================================================

LINKS_SCHEMA = StructType(
    [
        StructField("entity_type", StringType(), False),
        StructField("webid", StringType(), False),
        StructField("rel", StringType(), False),
        StructField("href", StringType(), True),
    ]
)


# =============================================================================
# Schema Mapping
# =============================================================================

TABLE_SCHEMAS: dict[str, StructType] = {
    # Discovery & Inventory
    TABLE_DATASERVERS: DATASERVERS_SCHEMA,
    TABLE_POINTS: POINTS_SCHEMA,
    TABLE_POINT_ATTRIBUTES: POINT_ATTRIBUTES_SCHEMA,
    TABLE_POINT_TYPE_CATALOG: POINT_TYPE_CATALOG_SCHEMA,
    # Time Series
    TABLE_TIMESERIES: TS_VALUE_SCHEMA,
    TABLE_STREAMSET_RECORDED: TS_VALUE_SCHEMA,
    TABLE_INTERPOLATED: TS_VALUE_SCHEMA,
    TABLE_STREAMSET_INTERPOLATED: TS_VALUE_SCHEMA,
    TABLE_PLOT: TS_VALUE_SCHEMA,
    TABLE_STREAMSET_PLOT: TS_VALUE_SCHEMA,
    TABLE_END: END_SCHEMA,
    TABLE_STREAMSET_END: END_SCHEMA,
    TABLE_CURRENT_VALUE: CURRENT_VALUE_SCHEMA,
    TABLE_VALUE_AT_TIME: VALUE_AT_TIME_SCHEMA,
    TABLE_RECORDED_AT_TIME: RECORDED_AT_TIME_SCHEMA,
    TABLE_SUMMARY: SUMMARY_SCHEMA,
    TABLE_STREAMSET_SUMMARY: STREAMSET_SUMMARY_SCHEMA,
    TABLE_CALCULATED: CALCULATED_SCHEMA,
    # Asset Framework
    TABLE_ASSET_SERVERS: ASSET_SERVERS_SCHEMA,
    TABLE_ASSET_DATABASES: ASSET_DATABASES_SCHEMA,
    TABLE_AF_HIERARCHY: AF_HIERARCHY_SCHEMA,
    TABLE_ELEMENT_ATTRIBUTES: ELEMENT_ATTRIBUTES_SCHEMA,
    TABLE_ELEMENT_TEMPLATES: ELEMENT_TEMPLATES_SCHEMA,
    TABLE_ELEMENT_TEMPLATE_ATTRIBUTES: ELEMENT_TEMPLATE_ATTRIBUTES_SCHEMA,
    TABLE_CATEGORIES: CATEGORIES_SCHEMA,
    TABLE_ATTRIBUTE_TEMPLATES: ATTRIBUTE_TEMPLATES_SCHEMA,
    TABLE_ANALYSES: ANALYSES_SCHEMA,
    TABLE_ANALYSIS_TEMPLATES: ANALYSIS_TEMPLATES_SCHEMA,
    TABLE_AF_TABLES: AF_TABLES_SCHEMA,
    TABLE_AF_TABLE_ROWS: AF_TABLE_ROWS_SCHEMA,
    TABLE_UNITS_OF_MEASURE: UNITS_OF_MEASURE_SCHEMA,
    # Event Frames
    TABLE_EVENT_FRAMES: EVENT_FRAMES_SCHEMA,
    TABLE_EVENTFRAME_ATTRIBUTES: EVENTFRAME_ATTRIBUTES_SCHEMA,
    TABLE_EVENTFRAME_TEMPLATES: EVENTFRAME_TEMPLATES_SCHEMA,
    TABLE_EVENTFRAME_TEMPLATE_ATTRIBUTES: EVENTFRAME_TEMPLATE_ATTRIBUTES_SCHEMA,
    TABLE_EVENTFRAME_REFERENCED_ELEMENTS: EVENTFRAME_REFERENCED_ELEMENTS_SCHEMA,
    TABLE_EVENTFRAME_ACKS: EVENTFRAME_ACKS_SCHEMA,
    TABLE_EVENTFRAME_ANNOTATIONS: EVENTFRAME_ANNOTATIONS_SCHEMA,
    # Governance & Diagnostics
    TABLE_LINKS: LINKS_SCHEMA,
}
"""Mapping of table names to their StructType schemas."""


# =============================================================================
# Table Metadata Definitions
# =============================================================================

TABLE_METADATA: dict[str, dict] = {
    # Discovery & Inventory (snapshot)
    TABLE_DATASERVERS: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_POINTS: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_POINT_ATTRIBUTES: {
        "primary_keys": ["point_webid", "name"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_POINT_TYPE_CATALOG: {
        "primary_keys": ["point_type", "engineering_units"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    # Time Series
    TABLE_TIMESERIES: {
        "primary_keys": ["tag_webid", "timestamp"],
        "cursor_field": "timestamp",
        "ingestion_type": "append",
    },
    TABLE_STREAMSET_RECORDED: {
        "primary_keys": ["tag_webid", "timestamp"],
        "cursor_field": "timestamp",
        "ingestion_type": "append",
    },
    TABLE_INTERPOLATED: {
        "primary_keys": ["tag_webid", "timestamp"],
        "cursor_field": "timestamp",
        "ingestion_type": "append",
    },
    TABLE_STREAMSET_INTERPOLATED: {
        "primary_keys": ["tag_webid", "timestamp"],
        "cursor_field": "timestamp",
        "ingestion_type": "append",
    },
    TABLE_PLOT: {
        "primary_keys": ["tag_webid", "timestamp"],
        "cursor_field": "timestamp",
        "ingestion_type": "append",
    },
    TABLE_STREAMSET_PLOT: {
        "primary_keys": ["tag_webid", "timestamp"],
        "cursor_field": "timestamp",
        "ingestion_type": "append",
    },
    TABLE_STREAMSET_SUMMARY: {
        "primary_keys": ["tag_webid", "summary_type", "timestamp"],
        "cursor_field": "timestamp",
        "ingestion_type": "append",
    },
    TABLE_CURRENT_VALUE: {
        "primary_keys": ["tag_webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_SUMMARY: {
        "primary_keys": ["tag_webid", "summary_type"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_END: {
        "primary_keys": ["tag_webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_STREAMSET_END: {
        "primary_keys": ["tag_webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_VALUE_AT_TIME: {
        "primary_keys": ["tag_webid", "timestamp"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_RECORDED_AT_TIME: {
        "primary_keys": ["tag_webid", "query_time"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_CALCULATED: {
        "primary_keys": ["tag_webid", "timestamp", "calculation_type"],
        "cursor_field": "timestamp",
        "ingestion_type": "append",
    },
    # Asset Framework (snapshot)
    TABLE_ASSET_SERVERS: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_ASSET_DATABASES: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_AF_HIERARCHY: {
        "primary_keys": ["element_webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_ELEMENT_ATTRIBUTES: {
        "primary_keys": ["element_webid", "attribute_webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_UNITS_OF_MEASURE: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_ELEMENT_TEMPLATES: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_CATEGORIES: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_ATTRIBUTE_TEMPLATES: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_ANALYSES: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_ANALYSIS_TEMPLATES: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_AF_TABLES: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_AF_TABLE_ROWS: {
        "primary_keys": ["table_webid", "row_index"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_ELEMENT_TEMPLATE_ATTRIBUTES: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    # Event Frames
    TABLE_EVENT_FRAMES: {
        "primary_keys": ["event_frame_webid", "start_time"],
        "cursor_field": "start_time",
        "ingestion_type": "append",
    },
    TABLE_EVENTFRAME_ATTRIBUTES: {
        "primary_keys": ["event_frame_webid", "attribute_webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_EVENTFRAME_TEMPLATES: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_EVENTFRAME_TEMPLATE_ATTRIBUTES: {
        "primary_keys": ["webid"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_EVENTFRAME_REFERENCED_ELEMENTS: {
        "primary_keys": ["event_frame_webid", "element_webid"],
        "cursor_field": "start_time",
        "ingestion_type": "append",
    },
    TABLE_EVENTFRAME_ACKS: {
        "primary_keys": ["event_frame_webid", "ack_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_EVENTFRAME_ANNOTATIONS: {
        "primary_keys": ["event_frame_webid", "annotation_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    # Governance & Diagnostics
    TABLE_LINKS: {
        "primary_keys": ["entity_type", "webid", "rel"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}
"""Metadata for each table including primary keys, cursor field, and ingestion type."""
