"""Table name constants and groupings for the OSI PI connector.

This module contains all table name constants and logical groupings used
by the OSI PI Lakeflow connector.
"""

# =============================================================================
# Discovery & Inventory Tables
# =============================================================================
TABLE_DATASERVERS = "pi_dataservers"
TABLE_POINTS = "pi_points"
TABLE_POINT_ATTRIBUTES = "pi_point_attributes"
TABLE_POINT_TYPE_CATALOG = "pi_point_type_catalog"

# =============================================================================
# Time Series Tables
# =============================================================================
TABLE_TIMESERIES = "pi_timeseries"
TABLE_INTERPOLATED = "pi_interpolated"
TABLE_PLOT = "pi_plot"
TABLE_CURRENT_VALUE = "pi_current_value"
TABLE_SUMMARY = "pi_summary"
TABLE_END = "pi_end"
TABLE_VALUE_AT_TIME = "pi_value_at_time"
TABLE_RECORDED_AT_TIME = "pi_recorded_at_time"
TABLE_CALCULATED = "pi_calculated"

# StreamSet variants (multi-tag batch endpoints)
TABLE_STREAMSET_RECORDED = "pi_streamset_recorded"
TABLE_STREAMSET_INTERPOLATED = "pi_streamset_interpolated"
TABLE_STREAMSET_PLOT = "pi_streamset_plot"
TABLE_STREAMSET_SUMMARY = "pi_streamset_summary"
TABLE_STREAMSET_END = "pi_streamset_end"

# =============================================================================
# Asset Framework (AF) Tables
# =============================================================================
TABLE_ASSET_SERVERS = "pi_assetservers"
TABLE_ASSET_DATABASES = "pi_assetdatabases"
TABLE_AF_HIERARCHY = "pi_af_hierarchy"
TABLE_ELEMENT_ATTRIBUTES = "pi_element_attributes"
TABLE_ELEMENT_TEMPLATES = "pi_element_templates"
TABLE_ELEMENT_TEMPLATE_ATTRIBUTES = "pi_element_template_attributes"
TABLE_ATTRIBUTE_TEMPLATES = "pi_attribute_templates"
TABLE_CATEGORIES = "pi_categories"
TABLE_ANALYSES = "pi_analyses"
TABLE_ANALYSIS_TEMPLATES = "pi_analysis_templates"
TABLE_AF_TABLES = "pi_af_tables"
TABLE_AF_TABLE_ROWS = "pi_af_table_rows"
TABLE_UNITS_OF_MEASURE = "pi_units_of_measure"

# =============================================================================
# Event Frame Tables
# =============================================================================
TABLE_EVENT_FRAMES = "pi_event_frames"
TABLE_EVENTFRAME_ATTRIBUTES = "pi_eventframe_attributes"
TABLE_EVENTFRAME_TEMPLATES = "pi_eventframe_templates"
TABLE_EVENTFRAME_TEMPLATE_ATTRIBUTES = "pi_eventframe_template_attributes"
TABLE_EVENTFRAME_REFERENCED_ELEMENTS = "pi_eventframe_referenced_elements"
TABLE_EVENTFRAME_ACKS = "pi_eventframe_acknowledgements"
TABLE_EVENTFRAME_ANNOTATIONS = "pi_eventframe_annotations"

# =============================================================================
# Governance & Diagnostics Tables
# =============================================================================
TABLE_LINKS = "pi_links"

# =============================================================================
# Table Groups (for logical organization)
# =============================================================================
TABLES_DISCOVERY_INVENTORY = [
    TABLE_DATASERVERS,
    TABLE_POINTS,
    TABLE_POINT_ATTRIBUTES,
    TABLE_POINT_TYPE_CATALOG,
]

TABLES_TIME_SERIES = [
    TABLE_TIMESERIES,
    TABLE_STREAMSET_RECORDED,
    TABLE_INTERPOLATED,
    TABLE_STREAMSET_INTERPOLATED,
    TABLE_PLOT,
    TABLE_STREAMSET_PLOT,
    TABLE_SUMMARY,
    TABLE_STREAMSET_SUMMARY,
    TABLE_CURRENT_VALUE,
    TABLE_VALUE_AT_TIME,
    TABLE_RECORDED_AT_TIME,
    TABLE_END,
    TABLE_STREAMSET_END,
    TABLE_CALCULATED,
]

TABLES_ASSET_FRAMEWORK = [
    TABLE_ASSET_SERVERS,
    TABLE_ASSET_DATABASES,
    TABLE_AF_HIERARCHY,
    TABLE_ELEMENT_ATTRIBUTES,
    TABLE_ELEMENT_TEMPLATES,
    TABLE_ELEMENT_TEMPLATE_ATTRIBUTES,
    TABLE_ATTRIBUTE_TEMPLATES,
    TABLE_CATEGORIES,
    TABLE_ANALYSES,
    TABLE_ANALYSIS_TEMPLATES,
    TABLE_AF_TABLES,
    TABLE_AF_TABLE_ROWS,
    TABLE_UNITS_OF_MEASURE,
]

TABLES_EVENT_FRAMES = [
    TABLE_EVENT_FRAMES,
    TABLE_EVENTFRAME_ATTRIBUTES,
    TABLE_EVENTFRAME_TEMPLATES,
    TABLE_EVENTFRAME_TEMPLATE_ATTRIBUTES,
    TABLE_EVENTFRAME_REFERENCED_ELEMENTS,
    TABLE_EVENTFRAME_ACKS,
    TABLE_EVENTFRAME_ANNOTATIONS,
]

TABLES_GOVERNANCE_DIAGNOSTICS = [
    TABLE_LINKS,
]

# All supported tables
SUPPORTED_TABLES = (
    TABLES_DISCOVERY_INVENTORY
    + TABLES_TIME_SERIES
    + TABLES_ASSET_FRAMEWORK
    + TABLES_EVENT_FRAMES
    + TABLES_GOVERNANCE_DIAGNOSTICS
)
"""List of all table names supported by the OSI PI connector."""
