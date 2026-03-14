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
