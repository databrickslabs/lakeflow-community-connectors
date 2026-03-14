"""UK Core R4 profile overrides.

Only resources where UK Core adds meaningful schema differences are defined here.
All other resources fall back to base_r4 via the registry fallback chain.

Profile sources:
  UK Core Patient v2.6.1: https://fhir.hl7.org.uk/StructureDefinition/UKCore-Patient
  Extension URLs verified from:
    https://github.com/NHSDigital/FHIR-R4-UKCORE-STAGING-MAIN/structuredefinitions/UKCore-Patient.xml
    https://hl7.org/fhir/R4/extension-patient-interpreterrequired.html

Resources with UK Core entries here:
  - Patient: adds NHS number, ethnic category, birth sex, and other UK extensions

Resources that fall back to base_r4 (no meaningful UK Core schema differences):
  - All other 13 resources
"""

from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from databricks.labs.community_connector.sources.fhir.fhir_types import (
    CODEABLE_CONCEPT,
    extract_codeable_concept,
)
from databricks.labs.community_connector.sources.fhir.fhir_profile_registry import (
    _COMMON_FIELDS, register,
)
from databricks.labs.community_connector.sources.fhir.profiles.base_r4 import (
    _patient as _base_patient,
    _PATIENT_SCHEMA,
)


def _f(name: str, t, nullable: bool = True) -> StructField:
    return StructField(name, t, nullable=nullable)


# ── UK Core extension URL constants ──────────────────────────────────────────
# Verified from UKCore-Patient.xml in NHSDigital/FHIR-R4-UKCORE-STAGING-MAIN
# Each StructureDefinition XML confirms the <url value="..."> and value[x] type.

# valueCodeableConcept — verified from Extension-UKCore-EthnicCategory.xml
_EXT_ETHNIC_CATEGORY = "https://fhir.hl7.org.uk/StructureDefinition/Extension-UKCore-EthnicCategory"

# valueCodeableConcept — verified from Extension-UKCore-BirthSex.xml
_EXT_BIRTH_SEX = "https://fhir.hl7.org.uk/StructureDefinition/Extension-UKCore-BirthSex"

# Complex nested extension (sub-extensions: deathNotificationStatus, systemEffectiveDate)
# Top-level valueX not used directly; sub-extension deathNotificationStatus is valueCodeableConcept
# Verified from Extension-UKCore-DeathNotificationStatus.xml
_EXT_DEATH_NOTIFICATION_STATUS = "https://fhir.hl7.org.uk/StructureDefinition/Extension-UKCore-DeathNotificationStatus"

# valueBoolean — standard HL7 R4 extension (NOT a UK Core-specific URL).
# UK Core Patient profile references the base HL7 extension for interpreter required.
# Verified from: https://hl7.org/fhir/R4/extension-patient-interpreterrequired.html
# and UKCore-Patient.xml profile reference:
#   profile value="http://hl7.org/fhir/StructureDefinition/patient-interpreterRequired"
_EXT_INTERPRETER_REQUIRED = "http://hl7.org/fhir/StructureDefinition/patient-interpreterRequired"

# valueCodeableConcept — verified from Extension-UKCore-ResidentialStatus.xml
_EXT_RESIDENTIAL_STATUS = "https://fhir.hl7.org.uk/StructureDefinition/Extension-UKCore-ResidentialStatus"

# NHS number identifier system — verified: fixedUri in UKCore-Patient.xml
_NHS_NUMBER_SYSTEM = "https://fhir.nhs.uk/Id/nhs-number"


def _find_extension(extensions: list, url: str) -> dict | None:
    """Find first extension matching url in an extension list."""
    for ext in (extensions or []):
        if ext.get("url") == url:
            return ext
    return None


# ─── Patient (UK Core) ────────────────────────────────────────────────────────
# UK Core adds NHS number and UK-specific extensions on top of base_r4 Patient
_PATIENT_UK_SCHEMA = StructType(
    list(_PATIENT_SCHEMA.fields) + [
        _f("nhs_number", StringType()),
        _f("ethnic_category", CODEABLE_CONCEPT),
        _f("birth_sex", CODEABLE_CONCEPT),
        _f("death_notification_status", CODEABLE_CONCEPT),
        _f("interpreter_required", BooleanType()),
        _f("residential_status", CODEABLE_CONCEPT),
    ]
)


@register("Patient", "uk_core", _PATIENT_UK_SCHEMA)
def _patient_uk(r: dict) -> dict:
    result = _base_patient(r)

    nhs_number = None
    for ident in (r.get("identifier") or []):
        if ident.get("system") == _NHS_NUMBER_SYSTEM:
            nhs_number = ident.get("value")
            break

    extensions = r.get("extension") or []
    ethnic_ext = _find_extension(extensions, _EXT_ETHNIC_CATEGORY)
    birth_sex_ext = _find_extension(extensions, _EXT_BIRTH_SEX)
    death_ext = _find_extension(extensions, _EXT_DEATH_NOTIFICATION_STATUS)
    interp_ext = _find_extension(extensions, _EXT_INTERPRETER_REQUIRED)
    residential_ext = _find_extension(extensions, _EXT_RESIDENTIAL_STATUS)

    # DeathNotificationStatus is a complex extension; extract the nested
    # deathNotificationStatus sub-extension's valueCodeableConcept.
    death_status_cc = None
    if death_ext:
        for sub_ext in (death_ext.get("extension") or []):
            if sub_ext.get("url") == "deathNotificationStatus":
                death_status_cc = sub_ext.get("valueCodeableConcept")
                break

    result.update({
        "nhs_number": nhs_number,
        "ethnic_category": extract_codeable_concept(
            ethnic_ext.get("valueCodeableConcept") if ethnic_ext else None
        ),
        "birth_sex": extract_codeable_concept(
            birth_sex_ext.get("valueCodeableConcept") if birth_sex_ext else None
        ),
        "death_notification_status": extract_codeable_concept(death_status_cc),
        "interpreter_required": (
            interp_ext.get("valueBoolean") if interp_ext else None
        ),
        "residential_status": extract_codeable_concept(
            residential_ext.get("valueCodeableConcept") if residential_ext else None
        ),
    })
    return result
