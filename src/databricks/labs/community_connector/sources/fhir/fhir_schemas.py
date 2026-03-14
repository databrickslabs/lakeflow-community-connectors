"""Spark StructType schemas for FHIR R4 resources.

Hybrid approach: common typed fields (id, resourceType, lastUpdated, raw_json)
on every resource, plus resource-specific typed fields for key attributes.
raw_json stores the full FHIR resource as a JSON string for lossless storage.

All fields are nullable — FHIR R4 has very few required fields (meta.lastUpdated
is 0..1; even id is 0..1 in spec though REST servers always populate it).
"""

from pyspark.sql.types import (
    BooleanType, DoubleType, StringType,
    StructField, StructType, TimestampType,
)

_COMMON_FIELDS = [
    StructField("id", StringType(), nullable=True),
    StructField("resourceType", StringType(), nullable=True),
    StructField("lastUpdated", TimestampType(), nullable=True),
    StructField("raw_json", StringType(), nullable=True),
]

def _s(*extra: StructField) -> StructType:
    return StructType(_COMMON_FIELDS + list(extra))

def _f(name: str, t, nullable: bool = True) -> StructField:
    return StructField(name, t, nullable=nullable)

FALLBACK_SCHEMA = StructType(_COMMON_FIELDS)

RESOURCE_SCHEMAS: dict[str, StructType] = {
    "Patient": _s(
        _f("gender", StringType()), _f("birthDate", StringType()),
        _f("active", BooleanType()), _f("name_text", StringType()),
        _f("name_family", StringType()),
    ),
    "Observation": _s(
        _f("status", StringType()), _f("code_text", StringType()),
        _f("code_system", StringType()), _f("code_code", StringType()),
        _f("subject_reference", StringType()), _f("effective_datetime", TimestampType()),
        _f("value_quantity_value", DoubleType()), _f("value_quantity_unit", StringType()),
        _f("value_string", StringType()), _f("issued", TimestampType()),
    ),
    "Condition": _s(
        _f("clinical_status", StringType()), _f("verification_status", StringType()),
        _f("code_text", StringType()), _f("subject_reference", StringType()),
        _f("onset_datetime", TimestampType()), _f("recorded_date", StringType()),
    ),
    "Encounter": _s(
        _f("status", StringType()), _f("class_code", StringType()),
        _f("subject_reference", StringType()), _f("period_start", TimestampType()),
        _f("period_end", TimestampType()),
    ),
    "Procedure": _s(
        _f("status", StringType()), _f("code_text", StringType()),
        _f("subject_reference", StringType()), _f("performed_datetime", TimestampType()),
    ),
    "MedicationRequest": _s(
        _f("status", StringType()), _f("intent", StringType()),
        _f("medication_text", StringType()), _f("subject_reference", StringType()),
        _f("authored_on", StringType()),
    ),
    "DiagnosticReport": _s(
        _f("status", StringType()), _f("code_text", StringType()),
        _f("subject_reference", StringType()), _f("effective_datetime", TimestampType()),
        _f("issued", TimestampType()),
    ),
    "AllergyIntolerance": _s(
        _f("clinical_status", StringType()), _f("verification_status", StringType()),
        _f("code_text", StringType()), _f("patient_reference", StringType()),
        _f("recorded_date", StringType()),
    ),
    "Immunization": _s(
        _f("status", StringType()), _f("vaccine_code_text", StringType()),
        _f("patient_reference", StringType()), _f("occurrence_datetime", TimestampType()),
    ),
    "Coverage": _s(
        _f("status", StringType()), _f("beneficiary_reference", StringType()),
        _f("payor_reference", StringType()), _f("period_start", TimestampType()),
        _f("period_end", TimestampType()),
    ),
    "CarePlan": _s(
        _f("status", StringType()), _f("intent", StringType()),
        _f("subject_reference", StringType()), _f("period_start", TimestampType()),
        _f("period_end", TimestampType()),
    ),
    "Goal": _s(
        _f("lifecycle_status", StringType()), _f("description_text", StringType()),
        _f("subject_reference", StringType()), _f("start_date", StringType()),
    ),
    "Device": _s(
        _f("status", StringType()), _f("device_name", StringType()),
        _f("type_text", StringType()), _f("patient_reference", StringType()),
    ),
    "DocumentReference": _s(
        _f("status", StringType()), _f("type_text", StringType()),
        _f("subject_reference", StringType()), _f("date", TimestampType()),
    ),
}

def get_schema(resource_type: str) -> StructType:
    """Return the Spark schema for the given FHIR resource type.
    Falls back to FALLBACK_SCHEMA for unknown resource types."""
    return RESOURCE_SCHEMAS.get(resource_type, FALLBACK_SCHEMA)
