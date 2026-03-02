"""
PySpark StructType schema definitions for the three DICOM hierarchy levels.

These schemas align with the DICOM Information Object hierarchy:
    Study → Series → Instance (SOP)
"""

from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
    VariantType,
)

# ---------------------------------------------------------------------------
# Study-level schema
# QIDO-RS /studies
# ---------------------------------------------------------------------------

STUDIES_SCHEMA = StructType(
    [
        StructField("StudyInstanceUID", StringType(), nullable=False),
        StructField("PatientID", StringType(), nullable=True),
        StructField("PatientName", StringType(), nullable=True),
        StructField("StudyDate", StringType(), nullable=True),
        StructField("StudyTime", StringType(), nullable=True),
        StructField("AccessionNumber", StringType(), nullable=True),
        StructField("StudyDescription", StringType(), nullable=True),
        # CS VR — can contain multiple modalities, e.g. ["CT", "SR"]
        StructField("ModalitiesInStudy", ArrayType(StringType()), nullable=True),
        StructField("NumberOfStudyRelatedSeries", LongType(), nullable=True),
        StructField("NumberOfStudyRelatedInstances", LongType(), nullable=True),
        # Lineage: UC connection name that produced this record
        StructField("connection_name", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Series-level schema
# QIDO-RS /series
# ---------------------------------------------------------------------------

SERIES_SCHEMA = StructType(
    [
        StructField("SeriesInstanceUID", StringType(), nullable=False),
        StructField("StudyInstanceUID", StringType(), nullable=True),
        StructField("StudyDate", StringType(), nullable=True),
        StructField("SeriesNumber", LongType(), nullable=True),
        StructField("SeriesDescription", StringType(), nullable=True),
        StructField("Modality", StringType(), nullable=True),
        StructField("BodyPartExamined", StringType(), nullable=True),
        StructField("SeriesDate", StringType(), nullable=True),
        # Lineage: UC connection name that produced this record
        StructField("connection_name", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Instance (SOP) level schema
# QIDO-RS /instances
# ---------------------------------------------------------------------------

INSTANCES_SCHEMA = StructType(
    [
        StructField("SOPInstanceUID", StringType(), nullable=False),
        StructField("SeriesInstanceUID", StringType(), nullable=True),
        StructField("StudyInstanceUID", StringType(), nullable=True),
        StructField("SOPClassUID", StringType(), nullable=True),
        StructField("InstanceNumber", LongType(), nullable=True),
        StructField("StudyDate", StringType(), nullable=True),
        StructField("ContentDate", StringType(), nullable=True),
        StructField("ContentTime", StringType(), nullable=True),
        # Populated when fetch_dicom_files=true; path inside Unity Catalog Volume
        StructField("dicom_file_path", StringType(), nullable=True),
        # Full DICOM JSON for this instance; populated when fetch_metadata=true.
        # Stored as VariantType; parse_value() converts the JSON string → VariantVal.
        StructField("metadata", VariantType(), nullable=True),
        # Lineage: UC connection name that produced this record
        StructField("connection_name", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Diagnostics schema
# WADO-RS / QIDO-RS capability probe results
# ---------------------------------------------------------------------------

DIAGNOSTICS_SCHEMA = StructType(
    [
        # Endpoint pattern, e.g. "/studies/{uid}/series/{uid}/instances/{uid}/frames/{n}"
        StructField("endpoint", StringType(), nullable=False),
        # Service category: QIDO-RS or WADO-RS
        StructField("category", StringType(), nullable=True),
        # Human-readable description of what the endpoint does
        StructField("description", StringType(), nullable=True),
        # "yes", "no", "unknown", or "error"
        StructField("supported", StringType(), nullable=False),
        # HTTP status code returned by the probe request (null on network error)
        StructField("status_code", LongType(), nullable=True),
        # Content-Type header from the response
        StructField("content_type", StringType(), nullable=True),
        # Round-trip latency in milliseconds
        StructField("latency_ms", LongType(), nullable=True),
        # Additional context: error message, Access Denied reason, etc.
        StructField("notes", StringType(), nullable=True),
        # ISO-8601 timestamp of this probe run (UTC)
        StructField("probe_timestamp", StringType(), nullable=False),
        # Lineage: UC connection name that produced this record
        StructField("connection_name", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

TABLE_SCHEMAS: dict[str, StructType] = {
    "studies": STUDIES_SCHEMA,
    "series": SERIES_SCHEMA,
    "instances": INSTANCES_SCHEMA,
    "diagnostics": DIAGNOSTICS_SCHEMA,
}


def get_schema(table_name: str) -> StructType:
    """Return the PySpark StructType for the given table name."""
    try:
        return TABLE_SCHEMAS[table_name]
    except KeyError:
        raise ValueError(f"Unknown table '{table_name}'. Valid tables: {list(TABLE_SCHEMAS)}")
