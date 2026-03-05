"""Schemas and metadata for the Google Sheets/Docs connector.

Defines Spark StructTypes and table metadata (primary keys, cursor field,
ingestion type) for the three tables: spreadsheets, sheet_values, documents.
When sheet_values uses first-row-as-header, the connector builds a dynamic
schema from the sheet; SHEET_VALUES_SCHEMA is the fallback (row_index + values array).
"""

from pyspark.sql.types import (
    ArrayType,
    StructField,
    StructType,
    StringType,
)

# ---------------------------------------------------------------------------
# Supported tables (used by list_tables and validation)
# ---------------------------------------------------------------------------
SUPPORTED_TABLES = ["spreadsheets", "sheet_values", "documents"]

# ---------------------------------------------------------------------------
# Static schemas (Spark StructType) per table
# ---------------------------------------------------------------------------

# spreadsheets: Drive API files.list result for mimeType=spreadsheet.
# id is the Drive file id (same as spreadsheetId for Sheets API).
SPREADSHEETS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("mimeType", StringType(), nullable=True),
        StructField("modifiedTime", StringType(), nullable=True),
        StructField("createdTime", StringType(), nullable=True),
    ]
)

# sheet_values: Default schema when not using first-row-as-header.
# One row per sheet row; values is an array of cell strings. When
# use_first_row_as_header is true, the connector returns a dynamic
# schema (row_index + named columns) instead.
SHEET_VALUES_SCHEMA = StructType(
    [
        StructField("row_index", StringType(), nullable=True),
        StructField("values", ArrayType(StringType(), True), nullable=True),
    ]
)

# documents: Drive file list for mimeType=document; content is populated
# when include_content is true (plain text via Drive export).
DOCUMENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("mimeType", StringType(), nullable=True),
        StructField("modifiedTime", StringType(), nullable=True),
        StructField("createdTime", StringType(), nullable=True),
        StructField("content", StringType(), nullable=True),
    ]
)

TABLE_SCHEMAS = {
    "spreadsheets": SPREADSHEETS_SCHEMA,
    "sheet_values": SHEET_VALUES_SCHEMA,
    "documents": DOCUMENTS_SCHEMA,
}

# ---------------------------------------------------------------------------
# Table metadata for Lakeflow pipeline (primary keys, cursor, ingestion type)
# ---------------------------------------------------------------------------
# sheet_values primary_keys are set dynamically to [first_column] when
# use_first_row_as_header is true and the first row can be fetched.
TABLE_METADATA = {
    "spreadsheets": {
        "primary_keys": ["id"],
        "cursor_field": "modifiedTime",
        "ingestion_type": "snapshot",
    },
    "sheet_values": {
        "primary_keys": [],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "documents": {
        "primary_keys": ["id"],
        "cursor_field": "modifiedTime",
        "ingestion_type": "snapshot",
    },
}
