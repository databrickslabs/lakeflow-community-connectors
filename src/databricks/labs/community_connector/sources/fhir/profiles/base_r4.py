"""FHIR R4 base profile — schemas and extractors for all 14 resources.

All schemas validated against the official FHIR R4 specification:
https://hl7.org/fhir/R4/

Each resource section references its exact spec URL.
Element names use FHIR JSON camelCase (e.g. effectiveDateTime, bodySite).
Column names in extractors use snake_case (e.g. effective_datetime, body_site).

Usage: imported by fhir_schemas.py to trigger @register decorators.
Do NOT import base_r4 from profiles/__init__.py (would cause circular import).
"""

from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)

from databricks.labs.community_connector.sources.fhir.fhir_types import (
    ADDRESS, ANNOTATION, CODEABLE_CONCEPT, CODING, CONTACT_POINT, DOSAGE,
    HUMAN_NAME, IDENTIFIER, PERIOD, QUANTITY, REFERENCE,
    extract_address, extract_annotation, extract_codeable_concept,
    extract_contact_point, extract_dosage, extract_human_name,
    extract_identifier, extract_period, extract_quantity, extract_reference,
    _safe,
)
from databricks.labs.community_connector.sources.fhir.profiles import (
    _COMMON_FIELDS, register,
)


def _f(name: str, t, nullable: bool = True) -> StructField:
    return StructField(name, t, nullable=nullable)


def _s(*extra: StructField) -> StructType:
    return StructType(list(_COMMON_FIELDS) + list(extra))


# ─── Patient ──────────────────────────────────────────────────────────────────
# FHIR R4:  https://hl7.org/fhir/R4/patient.html
# UK Core:  https://fhir.hl7.org.uk/StructureDefinition/UKCore-Patient v2.6.1
# MS fields: identifier, active, name, telecom, gender, birthDate, address,
#            managingOrganization
# deceased[x]: deceasedBoolean (boolean) | deceasedDateTime (dateTime)
_PATIENT_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("active", BooleanType()),
    _f("name", ArrayType(HUMAN_NAME)),
    _f("telecom", ArrayType(CONTACT_POINT)),
    _f("gender", StringType()),
    _f("birthDate", StringType()),
    _f("deceased_boolean", BooleanType()),
    _f("deceased_datetime", TimestampType()),
    _f("address", ArrayType(ADDRESS)),
    _f("maritalStatus", CODEABLE_CONCEPT),
    _f("generalPractitioner", ArrayType(REFERENCE)),
    _f("managingOrganization", REFERENCE),
)


@register("Patient", "base_r4", _PATIENT_SCHEMA)
def _patient(r: dict) -> dict:
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "active": r.get("active"),
        "name": [extract_human_name(n) for n in (r.get("name") or [])],
        "telecom": [extract_contact_point(t) for t in (r.get("telecom") or [])],
        "gender": r.get("gender"),
        "birthDate": r.get("birthDate"),
        "deceased_boolean": r.get("deceasedBoolean"),
        "deceased_datetime": r.get("deceasedDateTime"),
        "address": [extract_address(a) for a in (r.get("address") or [])],
        "maritalStatus": extract_codeable_concept(r.get("maritalStatus")),
        "generalPractitioner": [extract_reference(gp) for gp in (r.get("generalPractitioner") or [])],
        "managingOrganization": extract_reference(r.get("managingOrganization")),
    }


# ─── Observation ──────────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/observation.html
# UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Observation v2.5.0
# MS fields: status(R), category, code(R), subject, effective[x], performer, value[x], component
# value[x] variants: valueQuantity, valueCodeableConcept, valueString, valueBoolean, valueInteger
# effective[x] variants: effectiveDateTime, effectivePeriod

_OBS_REF_RANGE = StructType([
    _f("low_value", DoubleType()),
    _f("low_unit", StringType()),
    _f("high_value", DoubleType()),
    _f("high_unit", StringType()),
    _f("type", CODEABLE_CONCEPT),
    _f("text", StringType()),
])

_OBS_COMPONENT = StructType([
    _f("code", CODEABLE_CONCEPT),
    _f("value_quantity", QUANTITY),
    _f("value_string", StringType()),
    _f("value_codeable_concept", CODEABLE_CONCEPT),
    _f("value_boolean", BooleanType()),
    _f("data_absent_reason", CODEABLE_CONCEPT),
])

_OBSERVATION_SCHEMA = _s(
    _f("status", StringType()),
    _f("category", ArrayType(CODEABLE_CONCEPT)),
    _f("code", CODEABLE_CONCEPT),
    _f("subject", REFERENCE),
    _f("encounter", REFERENCE),
    _f("effective_datetime", TimestampType()),
    _f("effective_period", PERIOD),
    _f("issued", TimestampType()),
    _f("performer", ArrayType(REFERENCE)),
    _f("value_quantity", QUANTITY),
    _f("value_codeable_concept", CODEABLE_CONCEPT),
    _f("value_string", StringType()),
    _f("value_boolean", BooleanType()),
    _f("value_integer", IntegerType()),
    _f("data_absent_reason", CODEABLE_CONCEPT),
    _f("interpretation", ArrayType(CODEABLE_CONCEPT)),
    _f("body_site", CODEABLE_CONCEPT),
    _f("method", CODEABLE_CONCEPT),
    _f("specimen", REFERENCE),
    _f("device", REFERENCE),
    _f("reference_range", ArrayType(_OBS_REF_RANGE)),
    _f("has_member", ArrayType(REFERENCE)),
    _f("derived_from", ArrayType(REFERENCE)),
    _f("component", ArrayType(_OBS_COMPONENT)),
)


def _extract_obs_ref_range(obj: dict) -> dict:
    low = obj.get("low") or {}
    high = obj.get("high") or {}
    return {
        "low_value": low.get("value"),
        "low_unit": low.get("unit"),
        "high_value": high.get("value"),
        "high_unit": high.get("unit"),
        "type": extract_codeable_concept(obj.get("type")),
        "text": obj.get("text"),
    }


def _extract_obs_component(obj: dict) -> dict:
    return {
        "code": extract_codeable_concept(obj.get("code")),
        "value_quantity": extract_quantity(obj.get("valueQuantity")),
        "value_string": obj.get("valueString"),
        "value_codeable_concept": extract_codeable_concept(obj.get("valueCodeableConcept")),
        "value_boolean": obj.get("valueBoolean"),
        "data_absent_reason": extract_codeable_concept(obj.get("dataAbsentReason")),
    }


@register("Observation", "base_r4", _OBSERVATION_SCHEMA)
def _observation(r: dict) -> dict:
    return {
        "status": r.get("status"),
        "category": [extract_codeable_concept(c) for c in (r.get("category") or [])],
        "code": extract_codeable_concept(r.get("code")),
        "subject": extract_reference(r.get("subject")),
        "encounter": extract_reference(r.get("encounter")),
        "effective_datetime": r.get("effectiveDateTime"),
        "effective_period": extract_period(r.get("effectivePeriod")),
        "issued": r.get("issued"),
        "performer": [extract_reference(p) for p in (r.get("performer") or [])],
        "value_quantity": extract_quantity(r.get("valueQuantity")),
        "value_codeable_concept": extract_codeable_concept(r.get("valueCodeableConcept")),
        "value_string": r.get("valueString"),
        "value_boolean": r.get("valueBoolean"),
        "value_integer": r.get("valueInteger"),
        "data_absent_reason": extract_codeable_concept(r.get("dataAbsentReason")),
        "interpretation": [extract_codeable_concept(i) for i in (r.get("interpretation") or [])],
        "body_site": extract_codeable_concept(r.get("bodySite")),
        "method": extract_codeable_concept(r.get("method")),
        "specimen": extract_reference(r.get("specimen")),
        "device": extract_reference(r.get("device")),
        "reference_range": [_extract_obs_ref_range(rr) for rr in (r.get("referenceRange") or [])],
        "has_member": [extract_reference(m) for m in (r.get("hasMember") or [])],
        "derived_from": [extract_reference(d) for d in (r.get("derivedFrom") or [])],
        "component": [_extract_obs_component(c) for c in (r.get("component") or [])],
    }


