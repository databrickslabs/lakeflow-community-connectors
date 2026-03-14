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


# ─── Condition ────────────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/condition.html
# UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Condition v2.6.0
# MS fields: clinicalStatus, verificationStatus, severity, code, subject, recorder
# onset[x]: onsetDateTime | onsetPeriod | onsetString
# abatement[x]: abatementDateTime | abatementString
_CONDITION_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("clinical_status", CODEABLE_CONCEPT),
    _f("verification_status", CODEABLE_CONCEPT),
    _f("category", ArrayType(CODEABLE_CONCEPT)),
    _f("severity", CODEABLE_CONCEPT),
    _f("code", CODEABLE_CONCEPT),
    _f("body_site", ArrayType(CODEABLE_CONCEPT)),
    _f("subject", REFERENCE),
    _f("encounter", REFERENCE),
    _f("onset_datetime", TimestampType()),
    _f("onset_period", PERIOD),
    _f("onset_string", StringType()),
    _f("abatement_datetime", TimestampType()),
    _f("abatement_string", StringType()),
    _f("recorded_date", StringType()),
    _f("recorder", REFERENCE),
    _f("asserter", REFERENCE),
    _f("note", ArrayType(ANNOTATION)),
)


@register("Condition", "base_r4", _CONDITION_SCHEMA)
def _condition(r: dict) -> dict:
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "clinical_status": extract_codeable_concept(r.get("clinicalStatus")),
        "verification_status": extract_codeable_concept(r.get("verificationStatus")),
        "category": [extract_codeable_concept(c) for c in (r.get("category") or [])],
        "severity": extract_codeable_concept(r.get("severity")),
        "code": extract_codeable_concept(r.get("code")),
        "body_site": [extract_codeable_concept(b) for b in (r.get("bodySite") or [])],
        "subject": extract_reference(r.get("subject")),
        "encounter": extract_reference(r.get("encounter")),
        "onset_datetime": r.get("onsetDateTime"),
        "onset_period": extract_period(r.get("onsetPeriod")),
        "onset_string": r.get("onsetString"),
        "abatement_datetime": r.get("abatementDateTime"),
        "abatement_string": r.get("abatementString"),
        "recorded_date": r.get("recordedDate"),
        "recorder": extract_reference(r.get("recorder")),
        "asserter": extract_reference(r.get("asserter")),
        "note": [extract_annotation(n) for n in (r.get("note") or [])],
    }


# ─── AllergyIntolerance ───────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/allergyintolerance.html
# UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-AllergyIntolerance v2.5.0
# MS fields: clinicalStatus, verificationStatus, code(R), patient(R), reaction, reaction.severity
# NOTE: category is 0..* code (array of strings), not CodeableConcept — per spec
# onset[x]: onsetDateTime | onsetString
_ALLERGY_REACTION = StructType([
    _f("substance", CODEABLE_CONCEPT),
    _f("manifestation", ArrayType(CODEABLE_CONCEPT)),
    _f("description", StringType()),
    _f("onset", TimestampType()),
    _f("severity", StringType()),
    _f("exposure_route", CODEABLE_CONCEPT),
])

_ALLERGY_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("clinical_status", CODEABLE_CONCEPT),
    _f("verification_status", CODEABLE_CONCEPT),
    _f("type", StringType()),
    _f("category", ArrayType(StringType())),
    _f("criticality", StringType()),
    _f("code", CODEABLE_CONCEPT),
    _f("patient", REFERENCE),
    _f("encounter", REFERENCE),
    _f("onset_datetime", TimestampType()),
    _f("onset_string", StringType()),
    _f("recorded_date", StringType()),
    _f("recorder", REFERENCE),
    _f("asserter", REFERENCE),
    _f("last_occurrence", TimestampType()),
    _f("note", ArrayType(ANNOTATION)),
    _f("reaction", ArrayType(_ALLERGY_REACTION)),
)


def _extract_allergy_reaction(obj: dict) -> dict:
    return {
        "substance": extract_codeable_concept(obj.get("substance")),
        "manifestation": [extract_codeable_concept(m) for m in (obj.get("manifestation") or [])],
        "description": obj.get("description"),
        "onset": obj.get("onset"),
        "severity": obj.get("severity"),
        "exposure_route": extract_codeable_concept(obj.get("exposureRoute")),
    }


@register("AllergyIntolerance", "base_r4", _ALLERGY_SCHEMA)
def _allergy(r: dict) -> dict:
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "clinical_status": extract_codeable_concept(r.get("clinicalStatus")),
        "verification_status": extract_codeable_concept(r.get("verificationStatus")),
        "type": r.get("type"),
        "category": r.get("category") or [],
        "criticality": r.get("criticality"),
        "code": extract_codeable_concept(r.get("code")),
        "patient": extract_reference(r.get("patient")),
        "encounter": extract_reference(r.get("encounter")),
        "onset_datetime": r.get("onsetDateTime"),
        "onset_string": r.get("onsetString"),
        "recorded_date": r.get("recordedDate"),
        "recorder": extract_reference(r.get("recorder")),
        "asserter": extract_reference(r.get("asserter")),
        "last_occurrence": r.get("lastOccurrence"),
        "note": [extract_annotation(n) for n in (r.get("note") or [])],
        "reaction": [_extract_allergy_reaction(rx) for rx in (r.get("reaction") or [])],
    }


# ─── Encounter ────────────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/encounter.html
# UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Encounter v2.5.0
# MS fields: identifier, status(R), class(R Coding), serviceType, subject,
#            participant, reasonCode, reasonReference
# NOTE: Encounter.class is FHIR type Coding (1..1), NOT CodeableConcept

_ENCOUNTER_PARTICIPANT = StructType([
    _f("type", ArrayType(CODEABLE_CONCEPT)),
    _f("period", PERIOD),
    _f("individual", REFERENCE),
])

_ENCOUNTER_DIAGNOSIS = StructType([
    _f("condition", REFERENCE),
    _f("use", CODEABLE_CONCEPT),
    _f("rank", IntegerType()),
])

_ENCOUNTER_HOSPITALIZATION = StructType([
    _f("admit_source", CODEABLE_CONCEPT),
    _f("re_admission", CODEABLE_CONCEPT),
    _f("discharge_disposition", CODEABLE_CONCEPT),
    _f("origin", REFERENCE),
    _f("destination", REFERENCE),
])

_ENCOUNTER_LOCATION = StructType([
    _f("location", REFERENCE),
    _f("status", StringType()),
    _f("physical_type", CODEABLE_CONCEPT),
    _f("period", PERIOD),
])

_ENCOUNTER_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("status", StringType()),
    _f("class_coding", CODING),
    _f("type", ArrayType(CODEABLE_CONCEPT)),
    _f("service_type", CODEABLE_CONCEPT),
    _f("priority", CODEABLE_CONCEPT),
    _f("subject", REFERENCE),
    _f("episode_of_care", ArrayType(REFERENCE)),
    _f("participant", ArrayType(_ENCOUNTER_PARTICIPANT)),
    _f("appointment", ArrayType(REFERENCE)),
    _f("period", PERIOD),
    _f("reason_code", ArrayType(CODEABLE_CONCEPT)),
    _f("reason_reference", ArrayType(REFERENCE)),
    _f("diagnosis", ArrayType(_ENCOUNTER_DIAGNOSIS)),
    _f("hospitalization", _ENCOUNTER_HOSPITALIZATION),
    _f("location", ArrayType(_ENCOUNTER_LOCATION)),
    _f("service_provider", REFERENCE),
    _f("part_of", REFERENCE),
)


def _extract_encounter_participant(obj: dict) -> dict:
    return {
        "type": [extract_codeable_concept(t) for t in (obj.get("type") or [])],
        "period": extract_period(obj.get("period")),
        "individual": extract_reference(obj.get("individual")),
    }


def _extract_encounter_diagnosis(obj: dict) -> dict:
    return {
        "condition": extract_reference(obj.get("condition")),
        "use": extract_codeable_concept(obj.get("use")),
        "rank": obj.get("rank"),
    }


def _extract_encounter_hospitalization(obj: dict | None) -> dict | None:
    if not obj:
        return None
    return {
        "admit_source": extract_codeable_concept(obj.get("admitSource")),
        "re_admission": extract_codeable_concept(obj.get("reAdmission")),
        "discharge_disposition": extract_codeable_concept(obj.get("dischargeDisposition")),
        "origin": extract_reference(obj.get("origin")),
        "destination": extract_reference(obj.get("destination")),
    }


def _extract_encounter_location(obj: dict) -> dict:
    return {
        "location": extract_reference(obj.get("location")),
        "status": obj.get("status"),
        "physical_type": extract_codeable_concept(obj.get("physicalType")),
        "period": extract_period(obj.get("period")),
    }


@register("Encounter", "base_r4", _ENCOUNTER_SCHEMA)
def _encounter(r: dict) -> dict:
    class_obj = r.get("class") or {}
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "status": r.get("status"),
        "class_coding": {
            "system": class_obj.get("system"),
            "code": class_obj.get("code"),
            "display": class_obj.get("display"),
        } if class_obj else None,
        "type": [extract_codeable_concept(t) for t in (r.get("type") or [])],
        "service_type": extract_codeable_concept(r.get("serviceType")),
        "priority": extract_codeable_concept(r.get("priority")),
        "subject": extract_reference(r.get("subject")),
        "episode_of_care": [extract_reference(e) for e in (r.get("episodeOfCare") or [])],
        "participant": [_extract_encounter_participant(p) for p in (r.get("participant") or [])],
        "appointment": [extract_reference(a) for a in (r.get("appointment") or [])],
        "period": extract_period(r.get("period")),
        "reason_code": [extract_codeable_concept(rc) for rc in (r.get("reasonCode") or [])],
        "reason_reference": [extract_reference(rr) for rr in (r.get("reasonReference") or [])],
        "diagnosis": [_extract_encounter_diagnosis(d) for d in (r.get("diagnosis") or [])],
        "hospitalization": _extract_encounter_hospitalization(r.get("hospitalization")),
        "location": [_extract_encounter_location(loc) for loc in (r.get("location") or [])],
        "service_provider": extract_reference(r.get("serviceProvider")),
        "part_of": extract_reference(r.get("partOf")),
    }


# ─── Procedure ────────────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/procedure.html
# UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Procedure v2.5.0
# MS fields: status(R), code, subject(R), performed[x]
# performed[x]: performedDateTime | performedPeriod
_PROCEDURE_PERFORMER = StructType([
    _f("function", CODEABLE_CONCEPT),
    _f("actor", REFERENCE),
    _f("on_behalf_of", REFERENCE),
])

_PROCEDURE_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("status", StringType()),
    _f("status_reason", CODEABLE_CONCEPT),
    _f("category", CODEABLE_CONCEPT),
    _f("code", CODEABLE_CONCEPT),
    _f("subject", REFERENCE),
    _f("encounter", REFERENCE),
    _f("performed_datetime", TimestampType()),
    _f("performed_period", PERIOD),
    _f("recorder", REFERENCE),
    _f("asserter", REFERENCE),
    _f("performer", ArrayType(_PROCEDURE_PERFORMER)),
    _f("location", REFERENCE),
    _f("reason_code", ArrayType(CODEABLE_CONCEPT)),
    _f("reason_reference", ArrayType(REFERENCE)),
    _f("body_site", ArrayType(CODEABLE_CONCEPT)),
    _f("outcome", CODEABLE_CONCEPT),
    _f("report", ArrayType(REFERENCE)),
    _f("complication", ArrayType(CODEABLE_CONCEPT)),
    _f("follow_up", ArrayType(CODEABLE_CONCEPT)),
    _f("note", ArrayType(ANNOTATION)),
)


@register("Procedure", "base_r4", _PROCEDURE_SCHEMA)
def _procedure(r: dict) -> dict:
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "status": r.get("status"),
        "status_reason": extract_codeable_concept(r.get("statusReason")),
        "category": extract_codeable_concept(r.get("category")),
        "code": extract_codeable_concept(r.get("code")),
        "subject": extract_reference(r.get("subject")),
        "encounter": extract_reference(r.get("encounter")),
        "performed_datetime": r.get("performedDateTime"),
        "performed_period": extract_period(r.get("performedPeriod")),
        "recorder": extract_reference(r.get("recorder")),
        "asserter": extract_reference(r.get("asserter")),
        "performer": [
            {
                "function": extract_codeable_concept(p.get("function")),
                "actor": extract_reference(p.get("actor")),
                "on_behalf_of": extract_reference(p.get("onBehalfOf")),
            }
            for p in (r.get("performer") or [])
        ],
        "location": extract_reference(r.get("location")),
        "reason_code": [extract_codeable_concept(rc) for rc in (r.get("reasonCode") or [])],
        "reason_reference": [extract_reference(rr) for rr in (r.get("reasonReference") or [])],
        "body_site": [extract_codeable_concept(b) for b in (r.get("bodySite") or [])],
        "outcome": extract_codeable_concept(r.get("outcome")),
        "report": [extract_reference(rp) for rp in (r.get("report") or [])],
        "complication": [extract_codeable_concept(c) for c in (r.get("complication") or [])],
        "follow_up": [extract_codeable_concept(f) for f in (r.get("followUp") or [])],
        "note": [extract_annotation(n) for n in (r.get("note") or [])],
    }
