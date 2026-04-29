"""
PySpark StructType schema definitions for the three DICOM hierarchy levels.

These schemas align with the DICOM Information Object hierarchy:
    Study → Series → Instance (SOP)
"""

from pyspark.sql.types import (  # pylint: disable=no-name-in-module
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
        StructField("study_instance_uid", StringType(), nullable=False),
        StructField("patient_id", StringType(), nullable=True),
        StructField("patient_name", StringType(), nullable=True),
        StructField("study_date", StringType(), nullable=True),
        StructField("study_time", StringType(), nullable=True),
        StructField("accession_number", StringType(), nullable=True),
        StructField("study_description", StringType(), nullable=True),
        # CS VR — can contain multiple modalities, e.g. ["CT", "SR"]
        StructField("modalities_in_study", ArrayType(StringType()), nullable=True),
        StructField("number_of_study_related_series", LongType(), nullable=True),
        StructField("number_of_study_related_instances", LongType(), nullable=True),
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
        StructField("series_instance_uid", StringType(), nullable=False),
        StructField("study_instance_uid", StringType(), nullable=True),
        StructField("study_date", StringType(), nullable=True),
        StructField("series_number", LongType(), nullable=True),
        StructField("series_description", StringType(), nullable=True),
        StructField("modality", StringType(), nullable=True),
        StructField("body_part_examined", StringType(), nullable=True),
        StructField("series_date", StringType(), nullable=True),
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
        StructField("sop_instance_uid", StringType(), nullable=False),
        StructField("series_instance_uid", StringType(), nullable=True),
        StructField("study_instance_uid", StringType(), nullable=True),
        StructField("sop_class_uid", StringType(), nullable=True),
        StructField("instance_number", LongType(), nullable=True),
        StructField("study_date", StringType(), nullable=True),
        StructField("content_date", StringType(), nullable=True),
        StructField("content_time", StringType(), nullable=True),
        # Populated when fetch_dicom_files=true; path inside Unity Catalog Volume
        StructField("dicom_file_path", StringType(), nullable=True),
        # Full DICOM JSON for this instance; populated when fetch_metadata=true.
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
