"""Shared Spark StructType building blocks and extractor helpers for FHIR R4.

All types validated against the official FHIR R4 datatype spec:
https://hl7.org/fhir/R4/datatypes.html

Each StructType mirrors the FHIR datatype definition.
All fields are nullable — FHIR has very few required fields on datatypes.
Extractor helpers map raw FHIR JSON dicts to schema-matching Python dicts.
Return None (not {}) for absent objects — Spark treats None as null.
"""

from pyspark.sql.types import (
    ArrayType, DoubleType, StringType,
    StructField, StructType, TimestampType,
)


def _f(name: str, t, nullable: bool = True) -> StructField:
    return StructField(name, t, nullable=nullable)


# ── Coding ────────────────────────────────────────────────────────────────────
# https://hl7.org/fhir/R4/datatypes.html#Coding
# Fields: system (0..1 uri), version (0..1), code (0..1), display (0..1), userSelected (0..1)
# We include system, code, display — the analytically relevant fields.
CODING = StructType([
    _f("system", StringType()),
    _f("code", StringType()),
    _f("display", StringType()),
])

# ── CodeableConcept ───────────────────────────────────────────────────────────
# https://hl7.org/fhir/R4/datatypes.html#CodeableConcept
# Fields: coding (0..*), text (0..1)
CODEABLE_CONCEPT = StructType([
    _f("coding", ArrayType(CODING)),
    _f("text", StringType()),
])

# ── Reference ─────────────────────────────────────────────────────────────────
# https://hl7.org/fhir/R4/references.html#Reference
# Fields: reference (0..1), type (0..1), identifier (0..1), display (0..1)
# We include reference and display — the analytically relevant fields.
REFERENCE = StructType([
    _f("reference", StringType()),
    _f("display", StringType()),
])

# ── Period ────────────────────────────────────────────────────────────────────
# https://hl7.org/fhir/R4/datatypes.html#Period
# Fields: start (0..1 dateTime), end (0..1 dateTime)
PERIOD = StructType([
    _f("start", TimestampType()),
    _f("end", TimestampType()),
])

# ── Identifier ────────────────────────────────────────────────────────────────
# https://hl7.org/fhir/R4/datatypes.html#Identifier
# Fields: use (0..1), type (0..1), system (0..1 uri), value (0..1), period (0..1), assigner (0..1)
# We include use, system, value — the analytically relevant fields.
IDENTIFIER = StructType([
    _f("system", StringType()),
    _f("value", StringType()),
    _f("use", StringType()),
])

# ── HumanName ────────────────────────────────────────────────────────────────
# https://hl7.org/fhir/R4/datatypes.html#HumanName
# Fields: use (0..1), text (0..1), family (0..1), given (0..*), prefix (0..*), suffix (0..*)
# We include use, text, family, given — the analytically relevant fields.
HUMAN_NAME = StructType([
    _f("family", StringType()),
    _f("given", ArrayType(StringType())),
    _f("text", StringType()),
    _f("use", StringType()),
])

# ── Address ───────────────────────────────────────────────────────────────────
# https://hl7.org/fhir/R4/datatypes.html#Address
# Fields: use (0..1), type (0..1), text (0..1), line (0..*), city (0..1),
#         district (0..1), state (0..1), postalCode (0..1), country (0..1), period (0..1)
ADDRESS = StructType([
    _f("use", StringType()),
    _f("line", ArrayType(StringType())),
    _f("city", StringType()),
    _f("district", StringType()),
    _f("postalCode", StringType()),
    _f("country", StringType()),
])

# ── ContactPoint ──────────────────────────────────────────────────────────────
# https://hl7.org/fhir/R4/datatypes.html#ContactPoint
# Fields: system (0..1), value (0..1), use (0..1), rank (0..1), period (0..1)
CONTACT_POINT = StructType([
    _f("system", StringType()),
    _f("value", StringType()),
    _f("use", StringType()),
])

# ── Quantity ─────────────────────────────────────────────────────────────────
# https://hl7.org/fhir/R4/datatypes.html#Quantity
# Fields: value (0..1 decimal), comparator (0..1), unit (0..1), system (0..1 uri), code (0..1)
QUANTITY = StructType([
    _f("value", DoubleType()),
    _f("unit", StringType()),
    _f("system", StringType()),
    _f("code", StringType()),
])

# ── Annotation ────────────────────────────────────────────────────────────────
# https://hl7.org/fhir/R4/datatypes.html#Annotation
# Fields: author[x] (0..1), time (0..1 dateTime), text (1..1 markdown)
ANNOTATION = StructType([
    _f("text", StringType()),
    _f("time", TimestampType()),
])

# ── Dosage ────────────────────────────────────────────────────────────────────
# https://hl7.org/fhir/R4/dosage.html
# Full spec has many fields. We extract the clinically essential subset:
# text, timing.code, doseAndRate[0].doseQuantity, route, site
DOSAGE = StructType([
    _f("text", StringType()),
    _f("timing_code", StringType()),
    _f("dose_value", DoubleType()),
    _f("dose_unit", StringType()),
    _f("route", CODEABLE_CONCEPT),
    _f("site", CODEABLE_CONCEPT),
])


# ─────────────────────────────────────────────────────────────────────────────
# Extractor helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe(d, *keys, default=None):
    """Safely navigate nested dicts. Returns default if any key is missing or non-dict."""
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
        if d is None:
            return default
    return d


def extract_coding(obj: dict | None) -> dict | None:
    """Extract a FHIR Coding datatype."""
    if not obj:
        return None
    return {"system": obj.get("system"), "code": obj.get("code"), "display": obj.get("display")}


def extract_codeable_concept(obj: dict | None) -> dict | None:
    """Extract a FHIR CodeableConcept datatype."""
    if not obj:
        return None
    return {
        "coding": [extract_coding(c) for c in (obj.get("coding") or []) if c],
        "text": obj.get("text"),
    }


def extract_reference(obj: dict | None) -> dict | None:
    """Extract a FHIR Reference datatype."""
    if not obj:
        return None
    return {"reference": obj.get("reference"), "display": obj.get("display")}


def extract_period(obj: dict | None) -> dict | None:
    """Extract a FHIR Period datatype."""
    if not obj:
        return None
    return {"start": obj.get("start"), "end": obj.get("end")}


def extract_identifier(obj: dict | None) -> dict | None:
    """Extract a FHIR Identifier datatype."""
    if not obj:
        return None
    return {"system": obj.get("system"), "value": obj.get("value"), "use": obj.get("use")}


def extract_human_name(obj: dict | None) -> dict | None:
    """Extract a FHIR HumanName datatype."""
    if not obj:
        return None
    return {
        "family": obj.get("family"),
        "given": obj.get("given") or [],
        "text": obj.get("text"),
        "use": obj.get("use"),
    }


def extract_address(obj: dict | None) -> dict | None:
    """Extract a FHIR Address datatype."""
    if not obj:
        return None
    return {
        "use": obj.get("use"),
        "line": obj.get("line") or [],
        "city": obj.get("city"),
        "district": obj.get("district"),
        "postalCode": obj.get("postalCode"),
        "country": obj.get("country"),
    }


def extract_contact_point(obj: dict | None) -> dict | None:
    """Extract a FHIR ContactPoint datatype."""
    if not obj:
        return None
    return {"system": obj.get("system"), "value": obj.get("value"), "use": obj.get("use")}


def extract_quantity(obj: dict | None) -> dict | None:
    """Extract a FHIR Quantity datatype."""
    if not obj:
        return None
    return {
        "value": obj.get("value"),
        "unit": obj.get("unit"),
        "system": obj.get("system"),
        "code": obj.get("code"),
    }


def extract_annotation(obj: dict | None) -> dict | None:
    """Extract a FHIR Annotation datatype."""
    if not obj:
        return None
    return {"text": obj.get("text"), "time": obj.get("time")}


def extract_dosage(obj: dict) -> dict:
    """Extract a FHIR Dosage backbone element into a flat dict."""
    timing = obj.get("timing") or {}
    timing_codings = _safe(timing, "code", "coding") or []
    timing_code = (timing_codings[0] or {}).get("code") if timing_codings else None
    dose_and_rate = (obj.get("doseAndRate") or [{}])[0] or {}
    dose_qty = dose_and_rate.get("doseQuantity") or {}
    return {
        "text": obj.get("text"),
        "timing_code": timing_code,
        "dose_value": dose_qty.get("value"),
        "dose_unit": dose_qty.get("unit"),
        "route": extract_codeable_concept(obj.get("route")),
        "site": extract_codeable_concept(obj.get("site")),
    }
