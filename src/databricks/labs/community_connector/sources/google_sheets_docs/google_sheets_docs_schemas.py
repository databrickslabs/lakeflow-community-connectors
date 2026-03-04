"""Schemas and metadata for the Google Sheets/Docs connector."""

from pyspark.sql.types import (
    ArrayType,
    StructField,
    StructType,
    StringType,
)

# Tables supported by the connector
SUPPORTED_TABLES = ["spreadsheets", "sheet_values", "documents"]

# Spreadsheets: Drive file list filtered by mimeType=spreadsheet; id = spreadsheetId
SPREADSHEETS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("mimeType", StringType(), nullable=True),
        StructField("modifiedTime", StringType(), nullable=True),
        StructField("createdTime", StringType(), nullable=True),
    ]
)

# Sheet values: one row per sheet row; values = array of cell values (dynamic columns as single array)
SHEET_VALUES_SCHEMA = StructType(
    [
        StructField("row_index", StringType(), nullable=True),
        StructField("values", ArrayType(StringType(), True), nullable=True),
    ]
)

# Documents: Drive file list filtered by mimeType=document + optional content
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
