"""FHIR R4 base profile — schemas and extractors for all 14 resources.

All schemas validated against the official FHIR R4 specification:
https://hl7.org/fhir/R4/

Each resource section references its exact spec URL.
Element names use FHIR JSON camelCase (e.g. effectiveDateTime, bodySite).
Column names in extractors use snake_case (e.g. effective_datetime, body_site).

Usage: imported by fhir_schemas.py to trigger @register decorators.
Do NOT import base_r4 from profiles/__init__.py (would cause circular import).
"""
# pylint: disable=too-many-lines

from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, LongType, StringType,
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
from databricks.labs.community_connector.sources.fhir.fhir_profile_registry import (
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
        "generalPractitioner": [
            extract_reference(gp)
            for gp in (r.get("generalPractitioner") or [])
        ],
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
    _f("value_integer", LongType()),
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
    _f("value_integer", LongType()),
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
        "value_integer": obj.get("valueInteger"),
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
    _f("rank", LongType()),
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


# ─── DiagnosticReport ─────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/diagnosticreport.html
# UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-DiagnosticReport v2.5.0
# MS fields: status(R), category, code(R), subject, encounter, issued, result
# effective[x]: effectiveDateTime | effectivePeriod
_DIAGNOSTIC_REPORT_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("based_on", ArrayType(REFERENCE)),
    _f("status", StringType()),
    _f("category", ArrayType(CODEABLE_CONCEPT)),
    _f("code", CODEABLE_CONCEPT),
    _f("subject", REFERENCE),
    _f("encounter", REFERENCE),
    _f("effective_datetime", TimestampType()),
    _f("effective_period", PERIOD),
    _f("issued", TimestampType()),
    _f("performer", ArrayType(REFERENCE)),
    _f("results_interpreter", ArrayType(REFERENCE)),
    _f("specimen", ArrayType(REFERENCE)),
    _f("result", ArrayType(REFERENCE)),
    _f("conclusion", StringType()),
    _f("conclusion_code", ArrayType(CODEABLE_CONCEPT)),
)


@register("DiagnosticReport", "base_r4", _DIAGNOSTIC_REPORT_SCHEMA)
def _diagnostic_report(r: dict) -> dict:
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "based_on": [extract_reference(b) for b in (r.get("basedOn") or [])],
        "status": r.get("status"),
        "category": [extract_codeable_concept(c) for c in (r.get("category") or [])],
        "code": extract_codeable_concept(r.get("code")),
        "subject": extract_reference(r.get("subject")),
        "encounter": extract_reference(r.get("encounter")),
        "effective_datetime": r.get("effectiveDateTime"),
        "effective_period": extract_period(r.get("effectivePeriod")),
        "issued": r.get("issued"),
        "performer": [extract_reference(p) for p in (r.get("performer") or [])],
        "results_interpreter": [
            extract_reference(ri)
            for ri in (r.get("resultsInterpreter") or [])
        ],
        "specimen": [extract_reference(s) for s in (r.get("specimen") or [])],
        "result": [extract_reference(res) for res in (r.get("result") or [])],
        "conclusion": r.get("conclusion"),
        "conclusion_code": [extract_codeable_concept(cc) for cc in (r.get("conclusionCode") or [])],
    }


# ─── MedicationRequest ────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/medicationrequest.html
# UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-MedicationRequest v2.5.0
# MS fields: identifier, status(R), intent(R), category, medication[x](R), subject(R),
#            authoredOn, requester, dosageInstruction, dispenseRequest, substitution
# medication[x]: medicationCodeableConcept | medicationReference

_DISPENSE_REQUEST = StructType([
    _f("validity_period", PERIOD),
    _f("number_of_repeats_allowed", LongType()),
    _f("quantity_value", DoubleType()),
    _f("quantity_unit", StringType()),
    _f("expected_supply_duration_value", DoubleType()),
    _f("expected_supply_duration_unit", StringType()),
])

_MEDICATION_REQUEST_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("status", StringType()),
    _f("status_reason", CODEABLE_CONCEPT),
    _f("intent", StringType()),
    _f("category", ArrayType(CODEABLE_CONCEPT)),
    _f("priority", StringType()),
    _f("medication_codeable_concept", CODEABLE_CONCEPT),
    _f("medication_reference", REFERENCE),
    _f("subject", REFERENCE),
    _f("encounter", REFERENCE),
    _f("authored_on", StringType()),
    _f("requester", REFERENCE),
    _f("reason_code", ArrayType(CODEABLE_CONCEPT)),
    _f("reason_reference", ArrayType(REFERENCE)),
    _f("note", ArrayType(ANNOTATION)),
    _f("dosage_instruction", ArrayType(DOSAGE)),
    _f("dispense_request", _DISPENSE_REQUEST),
    _f("substitution_allowed_boolean", BooleanType()),
)


@register("MedicationRequest", "base_r4", _MEDICATION_REQUEST_SCHEMA)
def _medication_request(r: dict) -> dict:
    dr = r.get("dispenseRequest") or {}
    dr_qty = dr.get("quantity") or {}
    dr_dur = dr.get("expectedSupplyDuration") or {}
    sub = r.get("substitution") or {}
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "status": r.get("status"),
        "status_reason": extract_codeable_concept(r.get("statusReason")),
        "intent": r.get("intent"),
        "category": [extract_codeable_concept(c) for c in (r.get("category") or [])],
        "priority": r.get("priority"),
        "medication_codeable_concept": extract_codeable_concept(r.get("medicationCodeableConcept")),
        "medication_reference": extract_reference(r.get("medicationReference")),
        "subject": extract_reference(r.get("subject")),
        "encounter": extract_reference(r.get("encounter")),
        "authored_on": r.get("authoredOn"),
        "requester": extract_reference(r.get("requester")),
        "reason_code": [extract_codeable_concept(rc) for rc in (r.get("reasonCode") or [])],
        "reason_reference": [extract_reference(rr) for rr in (r.get("reasonReference") or [])],
        "note": [extract_annotation(n) for n in (r.get("note") or [])],
        "dosage_instruction": [extract_dosage(d) for d in (r.get("dosageInstruction") or [])],
        "dispense_request": {
            "validity_period": extract_period(dr.get("validityPeriod")),
            "number_of_repeats_allowed": dr.get("numberOfRepeatsAllowed"),
            "quantity_value": dr_qty.get("value"),
            "quantity_unit": dr_qty.get("unit"),
            "expected_supply_duration_value": dr_dur.get("value"),
            "expected_supply_duration_unit": dr_dur.get("unit"),
        } if dr else None,
        "substitution_allowed_boolean": sub.get("allowedBoolean"),
    }


# ─── Immunization ─────────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/immunization.html
# UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Immunization v2.4.0
# MS: status(R), vaccineCode(R), patient(R), occurrence[x](R), manufacturer, lotNumber, doseQuantity
# occurrence[x]: occurrenceDateTime | occurrenceString

_IMMUNIZATION_PERFORMER = StructType([
    _f("function", CODEABLE_CONCEPT),
    _f("actor", REFERENCE),
])

_PROTOCOL_APPLIED = StructType([
    _f("series", StringType()),
    _f("authority", REFERENCE),
    _f("target_disease", ArrayType(CODEABLE_CONCEPT)),
    _f("dose_number_positive_int", LongType()),
    _f("dose_number_string", StringType()),
    _f("series_doses_positive_int", LongType()),
    _f("series_doses_string", StringType()),
])

_IMMUNIZATION_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("status", StringType()),
    _f("status_reason", CODEABLE_CONCEPT),
    _f("vaccine_code", CODEABLE_CONCEPT),
    _f("patient", REFERENCE),
    _f("encounter", REFERENCE),
    _f("occurrence_datetime", TimestampType()),
    _f("occurrence_string", StringType()),
    _f("recorded", StringType()),
    _f("primary_source", BooleanType()),
    _f("manufacturer", REFERENCE),
    _f("lot_number", StringType()),
    _f("expiration_date", StringType()),
    _f("site", CODEABLE_CONCEPT),
    _f("route", CODEABLE_CONCEPT),
    _f("dose_quantity", QUANTITY),
    _f("performer", ArrayType(_IMMUNIZATION_PERFORMER)),
    _f("reason_code", ArrayType(CODEABLE_CONCEPT)),
    _f("reason_reference", ArrayType(REFERENCE)),
    _f("is_subpotent", BooleanType()),
    _f("program_eligibility", ArrayType(CODEABLE_CONCEPT)),
    _f("funding_source", CODEABLE_CONCEPT),
    _f("protocol_applied", ArrayType(_PROTOCOL_APPLIED)),
)


def _extract_protocol_applied(obj: dict) -> dict:
    return {
        "series": obj.get("series"),
        "authority": extract_reference(obj.get("authority")),
        "target_disease": [extract_codeable_concept(td) for td in (obj.get("targetDisease") or [])],
        "dose_number_positive_int": obj.get("doseNumberPositiveInt"),
        "dose_number_string": obj.get("doseNumberString"),
        "series_doses_positive_int": obj.get("seriesDosesPositiveInt"),
        "series_doses_string": obj.get("seriesDosesString"),
    }


@register("Immunization", "base_r4", _IMMUNIZATION_SCHEMA)
def _immunization(r: dict) -> dict:
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "status": r.get("status"),
        "status_reason": extract_codeable_concept(r.get("statusReason")),
        "vaccine_code": extract_codeable_concept(r.get("vaccineCode")),
        "patient": extract_reference(r.get("patient")),
        "encounter": extract_reference(r.get("encounter")),
        "occurrence_datetime": r.get("occurrenceDateTime"),
        "occurrence_string": r.get("occurrenceString"),
        "recorded": r.get("recorded"),
        "primary_source": r.get("primarySource"),
        "manufacturer": extract_reference(r.get("manufacturer")),
        "lot_number": r.get("lotNumber"),
        "expiration_date": r.get("expirationDate"),
        "site": extract_codeable_concept(r.get("site")),
        "route": extract_codeable_concept(r.get("route")),
        "dose_quantity": extract_quantity(r.get("doseQuantity")),
        "performer": [
            {
                "function": extract_codeable_concept(p.get("function")),
                "actor": extract_reference(p.get("actor")),
            }
            for p in (r.get("performer") or [])
        ],
        "reason_code": [extract_codeable_concept(rc) for rc in (r.get("reasonCode") or [])],
        "reason_reference": [extract_reference(rr) for rr in (r.get("reasonReference") or [])],
        "is_subpotent": r.get("isSubpotent"),
        "program_eligibility": [
            extract_codeable_concept(pe)
            for pe in (r.get("programEligibility") or [])
        ],
        "funding_source": extract_codeable_concept(r.get("fundingSource")),
        "protocol_applied": [
            _extract_protocol_applied(pa)
            for pa in (r.get("protocolApplied") or [])
        ],
    }


# ─── Coverage ─────────────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/coverage.html
# No UK Core profile — base R4 is authoritative
# Required: status(R), beneficiary(R), payor(R 1..*)
# NOTE: FHIR JSON field is "class" — use r.get("class") in extractor
_COVERAGE_CLASS = StructType([
    _f("type", CODEABLE_CONCEPT),
    _f("value", StringType()),
    _f("name", StringType()),
])

_COVERAGE_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("status", StringType()),
    _f("type", CODEABLE_CONCEPT),
    _f("policy_holder", REFERENCE),
    _f("subscriber", REFERENCE),
    _f("subscriber_id", StringType()),
    _f("beneficiary", REFERENCE),
    _f("dependent", StringType()),
    _f("relationship", CODEABLE_CONCEPT),
    _f("period", PERIOD),
    _f("payor", ArrayType(REFERENCE)),
    _f("class_coverage", ArrayType(_COVERAGE_CLASS)),
    _f("order", LongType()),
    _f("network", StringType()),
    _f("subrogation", BooleanType()),
)


@register("Coverage", "base_r4", _COVERAGE_SCHEMA)
def _coverage(r: dict) -> dict:
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "status": r.get("status"),
        "type": extract_codeable_concept(r.get("type")),
        "policy_holder": extract_reference(r.get("policyHolder")),
        "subscriber": extract_reference(r.get("subscriber")),
        "subscriber_id": r.get("subscriberId"),
        "beneficiary": extract_reference(r.get("beneficiary")),
        "dependent": r.get("dependent"),
        "relationship": extract_codeable_concept(r.get("relationship")),
        "period": extract_period(r.get("period")),
        "payor": [extract_reference(p) for p in (r.get("payor") or [])],
        "class_coverage": [
            {
                "type": extract_codeable_concept(c.get("type")),
                "value": c.get("value"),
                "name": c.get("name"),
            }
            for c in (r.get("class") or [])
        ],
        "order": r.get("order"),
        "network": r.get("network"),
        "subrogation": r.get("subrogation"),
    }


# ─── CarePlan ─────────────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/careplan.html
# UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-CarePlan v2.2.0
# UK Core adds minimal constraints only. Required: status(R), intent(R), subject(R)
_CARE_PLAN_ACTIVITY = StructType([
    _f("reference", REFERENCE),
    _f("detail_status", StringType()),
    _f("detail_code", CODEABLE_CONCEPT),
    _f("detail_description", StringType()),
])

_CARE_PLAN_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("status", StringType()),
    _f("intent", StringType()),
    _f("category", ArrayType(CODEABLE_CONCEPT)),
    _f("title", StringType()),
    _f("description", StringType()),
    _f("subject", REFERENCE),
    _f("encounter", REFERENCE),
    _f("period", PERIOD),
    _f("created", StringType()),
    _f("author", REFERENCE),
    _f("care_team", ArrayType(REFERENCE)),
    _f("addresses", ArrayType(REFERENCE)),
    _f("goal", ArrayType(REFERENCE)),
    _f("activity", ArrayType(_CARE_PLAN_ACTIVITY)),
    _f("note", ArrayType(ANNOTATION)),
)


@register("CarePlan", "base_r4", _CARE_PLAN_SCHEMA)
def _care_plan(r: dict) -> dict:
    def _extract_activity(obj: dict) -> dict:
        detail = obj.get("detail") or {}
        return {
            "reference": extract_reference(obj.get("reference")),
            "detail_status": detail.get("status"),
            "detail_code": extract_codeable_concept(detail.get("code")),
            "detail_description": detail.get("description"),
        }
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "status": r.get("status"),
        "intent": r.get("intent"),
        "category": [extract_codeable_concept(c) for c in (r.get("category") or [])],
        "title": r.get("title"),
        "description": r.get("description"),
        "subject": extract_reference(r.get("subject")),
        "encounter": extract_reference(r.get("encounter")),
        "period": extract_period(r.get("period")),
        "created": r.get("created"),
        "author": extract_reference(r.get("author")),
        "care_team": [extract_reference(ct) for ct in (r.get("careTeam") or [])],
        "addresses": [extract_reference(a) for a in (r.get("addresses") or [])],
        "goal": [extract_reference(g) for g in (r.get("goal") or [])],
        "activity": [_extract_activity(a) for a in (r.get("activity") or [])],
        "note": [extract_annotation(n) for n in (r.get("note") or [])],
    }


# ─── Goal ─────────────────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/goal.html
# No UK Core profile — base R4 is authoritative
# Required: lifecycleStatus(R), description(R), subject(R)
# start[x]: startDate | startCodeableConcept
_GOAL_TARGET = StructType([
    _f("measure", CODEABLE_CONCEPT),
    _f("detail_quantity", QUANTITY),
    _f("detail_string", StringType()),
    _f("detail_boolean", BooleanType()),
    _f("due_date", StringType()),
])

_GOAL_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("lifecycle_status", StringType()),
    _f("achievement_status", CODEABLE_CONCEPT),
    _f("category", ArrayType(CODEABLE_CONCEPT)),
    _f("priority", CODEABLE_CONCEPT),
    _f("description", CODEABLE_CONCEPT),
    _f("subject", REFERENCE),
    _f("start_date", StringType()),
    _f("start_codeable_concept", CODEABLE_CONCEPT),
    _f("target", ArrayType(_GOAL_TARGET)),
    _f("status_date", StringType()),
    _f("status_reason", StringType()),
    _f("expressed_by", REFERENCE),
    _f("addresses", ArrayType(REFERENCE)),
    _f("note", ArrayType(ANNOTATION)),
)


def _extract_goal_target(obj: dict) -> dict:
    return {
        "measure": extract_codeable_concept(obj.get("measure")),
        "detail_quantity": extract_quantity(obj.get("detailQuantity")),
        "detail_string": obj.get("detailString"),
        "detail_boolean": obj.get("detailBoolean"),
        "due_date": obj.get("dueDate"),
    }


@register("Goal", "base_r4", _GOAL_SCHEMA)
def _goal(r: dict) -> dict:
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "lifecycle_status": r.get("lifecycleStatus"),
        "achievement_status": extract_codeable_concept(r.get("achievementStatus")),
        "category": [extract_codeable_concept(c) for c in (r.get("category") or [])],
        "priority": extract_codeable_concept(r.get("priority")),
        "description": extract_codeable_concept(r.get("description")),
        "subject": extract_reference(r.get("subject")),
        "start_date": r.get("startDate"),
        "start_codeable_concept": extract_codeable_concept(r.get("startCodeableConcept")),
        "target": [_extract_goal_target(t) for t in (r.get("target") or [])],
        "status_date": r.get("statusDate"),
        "status_reason": r.get("statusReason"),
        "expressed_by": extract_reference(r.get("expressedBy")),
        "addresses": [extract_reference(a) for a in (r.get("addresses") or [])],
        "note": [extract_annotation(n) for n in (r.get("note") or [])],
    }


# ─── Device ───────────────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/device.html
# UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Device v1.2.0
# MS fields (UK Core): status, type
_DEVICE_NAME = StructType([
    _f("name", StringType()),
    _f("type", StringType()),
])

_DEVICE_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("status", StringType()),
    _f("status_reason", ArrayType(CODEABLE_CONCEPT)),
    _f("manufacturer", StringType()),
    _f("manufacture_date", StringType()),
    _f("expiration_date", StringType()),
    _f("lot_number", StringType()),
    _f("serial_number", StringType()),
    _f("device_name", ArrayType(_DEVICE_NAME)),
    _f("model_number", StringType()),
    _f("type", CODEABLE_CONCEPT),
    _f("patient", REFERENCE),
    _f("owner", REFERENCE),
    _f("contact", ArrayType(CONTACT_POINT)),
    _f("note", ArrayType(ANNOTATION)),
    _f("safety", ArrayType(CODEABLE_CONCEPT)),
)


@register("Device", "base_r4", _DEVICE_SCHEMA)
def _device(r: dict) -> dict:
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "status": r.get("status"),
        "status_reason": [extract_codeable_concept(sr) for sr in (r.get("statusReason") or [])],
        "manufacturer": r.get("manufacturer"),
        "manufacture_date": r.get("manufactureDate"),
        "expiration_date": r.get("expirationDate"),
        "lot_number": r.get("lotNumber"),
        "serial_number": r.get("serialNumber"),
        "device_name": [
            {"name": dn.get("name"), "type": dn.get("type")}
            for dn in (r.get("deviceName") or [])
        ],
        "model_number": r.get("modelNumber"),
        "type": extract_codeable_concept(r.get("type")),
        "patient": extract_reference(r.get("patient")),
        "owner": extract_reference(r.get("owner")),
        "contact": [extract_contact_point(cp) for cp in (r.get("contact") or [])],
        "note": [extract_annotation(n) for n in (r.get("note") or [])],
        "safety": [extract_codeable_concept(s) for s in (r.get("safety") or [])],
    }


# ─── DocumentReference ────────────────────────────────────────────────────────
# FHIR R4: https://hl7.org/fhir/R4/documentreference.html
# UK Core: https://fhir.hl7.org.uk/StructureDefinition/UKCore-DocumentReference v2.2.0
# MS: identifier, status(R), type, category, subject, date, author, description, content(R 1..*)
_DOC_REF_RELATES_TO = StructType([
    _f("code", StringType()),
    _f("target", REFERENCE),
])

_DOC_REF_CONTENT = StructType([
    _f("attachment_url", StringType()),
    _f("attachment_title", StringType()),
    _f("attachment_content_type", StringType()),
    _f("format_code", StringType()),
    _f("format_system", StringType()),
])

_DOC_REF_CONTEXT = StructType([
    _f("encounter", ArrayType(REFERENCE)),
    _f("event", ArrayType(CODEABLE_CONCEPT)),
    _f("period", PERIOD),
    _f("facility_type", CODEABLE_CONCEPT),
    _f("practice_setting", CODEABLE_CONCEPT),
])

_DOCUMENT_REFERENCE_SCHEMA = _s(
    _f("identifier", ArrayType(IDENTIFIER)),
    _f("status", StringType()),
    _f("doc_status", StringType()),
    _f("type", CODEABLE_CONCEPT),
    _f("category", ArrayType(CODEABLE_CONCEPT)),
    _f("subject", REFERENCE),
    _f("date", TimestampType()),
    _f("author", ArrayType(REFERENCE)),
    _f("authenticator", REFERENCE),
    _f("custodian", REFERENCE),
    _f("relates_to", ArrayType(_DOC_REF_RELATES_TO)),
    _f("description", StringType()),
    _f("security_label", ArrayType(CODEABLE_CONCEPT)),
    _f("content", ArrayType(_DOC_REF_CONTENT)),
    _f("context", _DOC_REF_CONTEXT),
)


def _extract_doc_ref_content(obj: dict) -> dict:
    att = obj.get("attachment") or {}
    fmt = obj.get("format") or {}
    return {
        "attachment_url": att.get("url"),
        "attachment_title": att.get("title"),
        "attachment_content_type": att.get("contentType"),
        "format_code": fmt.get("code"),
        "format_system": fmt.get("system"),
    }


def _extract_doc_ref_context(obj: dict | None) -> dict | None:
    if not obj:
        return None
    return {
        "encounter": [extract_reference(e) for e in (obj.get("encounter") or [])],
        "event": [extract_codeable_concept(ev) for ev in (obj.get("event") or [])],
        "period": extract_period(obj.get("period")),
        "facility_type": extract_codeable_concept(obj.get("facilityType")),
        "practice_setting": extract_codeable_concept(obj.get("practiceSetting")),
    }


@register("DocumentReference", "base_r4", _DOCUMENT_REFERENCE_SCHEMA)
def _document_reference(r: dict) -> dict:
    return {
        "identifier": [extract_identifier(i) for i in (r.get("identifier") or [])],
        "status": r.get("status"),
        "doc_status": r.get("docStatus"),
        "type": extract_codeable_concept(r.get("type")),
        "category": [extract_codeable_concept(c) for c in (r.get("category") or [])],
        "subject": extract_reference(r.get("subject")),
        "date": r.get("date"),
        "author": [extract_reference(a) for a in (r.get("author") or [])],
        "authenticator": extract_reference(r.get("authenticator")),
        "custodian": extract_reference(r.get("custodian")),
        "relates_to": [
            {"code": rt.get("code"), "target": extract_reference(rt.get("target"))}
            for rt in (r.get("relatesTo") or [])
        ],
        "description": r.get("description"),
        "security_label": [extract_codeable_concept(sl) for sl in (r.get("securityLabel") or [])],
        "content": [_extract_doc_ref_content(c) for c in (r.get("content") or [])],
        "context": _extract_doc_ref_context(r.get("context")),
    }
