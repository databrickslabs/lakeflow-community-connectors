"""Tests for fhir_types.py — validates shared Spark type building blocks
and extractor helpers against FHIR R4 datatypes spec."""

from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, StringType, StructType, TimestampType,
)

from databricks.labs.community_connector.sources.fhir.fhir_types import (
    ADDRESS, ANNOTATION, CODEABLE_CONCEPT, CODING, CONTACT_POINT, DOSAGE,
    HUMAN_NAME, IDENTIFIER, PERIOD, QUANTITY, REFERENCE,
    extract_address, extract_codeable_concept, extract_contact_point,
    extract_dosage, extract_human_name, extract_identifier, extract_period,
    extract_quantity, extract_reference, _safe,
)


def test_coding_fields():
    names = {f.name for f in CODING.fields}
    assert names == {"system", "code", "display"}
    assert all(isinstance(f.dataType, StringType) for f in CODING.fields)


def test_codeable_concept_fields():
    names = {f.name for f in CODEABLE_CONCEPT.fields}
    assert names == {"coding", "text"}
    coding_f = next(f for f in CODEABLE_CONCEPT.fields if f.name == "coding")
    assert isinstance(coding_f.dataType, ArrayType)


def test_reference_fields():
    names = {f.name for f in REFERENCE.fields}
    assert names == {"reference", "display"}


def test_period_fields():
    names = {f.name for f in PERIOD.fields}
    assert names == {"start", "end"}
    for f in PERIOD.fields:
        assert isinstance(f.dataType, TimestampType)


def test_identifier_fields():
    names = {f.name for f in IDENTIFIER.fields}
    assert names == {"system", "value", "use"}


def test_human_name_fields():
    names = {f.name for f in HUMAN_NAME.fields}
    assert {"family", "given", "text", "use"} <= names
    given_f = next(f for f in HUMAN_NAME.fields if f.name == "given")
    assert isinstance(given_f.dataType, ArrayType)


def test_address_fields():
    names = {f.name for f in ADDRESS.fields}
    assert {"use", "line", "city", "district", "postalCode", "country"} <= names
    line_f = next(f for f in ADDRESS.fields if f.name == "line")
    assert isinstance(line_f.dataType, ArrayType)


def test_contact_point_fields():
    names = {f.name for f in CONTACT_POINT.fields}
    assert names == {"system", "value", "use"}


def test_quantity_fields():
    names = {f.name for f in QUANTITY.fields}
    assert {"value", "unit", "system", "code"} <= names
    val_f = next(f for f in QUANTITY.fields if f.name == "value")
    assert isinstance(val_f.dataType, DoubleType)


def test_annotation_fields():
    names = {f.name for f in ANNOTATION.fields}
    assert {"text", "time"} <= names


def test_dosage_fields():
    names = {f.name for f in DOSAGE.fields}
    assert {"text", "timing_code", "dose_value", "dose_unit", "route", "site"} <= names


def test_all_types_are_struct_types():
    for name, t in [
        ("CODING", CODING), ("CODEABLE_CONCEPT", CODEABLE_CONCEPT),
        ("REFERENCE", REFERENCE), ("PERIOD", PERIOD), ("IDENTIFIER", IDENTIFIER),
        ("HUMAN_NAME", HUMAN_NAME), ("ADDRESS", ADDRESS), ("CONTACT_POINT", CONTACT_POINT),
        ("QUANTITY", QUANTITY), ("ANNOTATION", ANNOTATION), ("DOSAGE", DOSAGE),
    ]:
        assert isinstance(t, StructType), f"{name} must be StructType"


def test_extract_codeable_concept_full():
    obj = {"coding": [{"system": "http://snomed.info/sct", "code": "73211009", "display": "Diabetes"}], "text": "Diabetes mellitus"}
    result = extract_codeable_concept(obj)
    assert result["text"] == "Diabetes mellitus"
    assert result["coding"][0]["code"] == "73211009"
    assert result["coding"][0]["system"] == "http://snomed.info/sct"


def test_extract_codeable_concept_none_returns_none():
    assert extract_codeable_concept(None) is None
    assert extract_codeable_concept({}) is None


def test_extract_reference():
    result = extract_reference({"reference": "Patient/123", "display": "John"})
    assert result["reference"] == "Patient/123"
    assert result["display"] == "John"


def test_extract_reference_none_returns_none():
    assert extract_reference(None) is None


def test_extract_period():
    result = extract_period({"start": "2024-01-01T00:00:00Z", "end": "2024-12-31T23:59:59Z"})
    assert result["start"] == "2024-01-01T00:00:00Z"
    assert result["end"] == "2024-12-31T23:59:59Z"


def test_extract_period_none_returns_none():
    assert extract_period(None) is None


def test_extract_identifier():
    result = extract_identifier({"system": "https://fhir.nhs.uk/Id/nhs-number", "value": "9000000009", "use": "official"})
    assert result["system"] == "https://fhir.nhs.uk/Id/nhs-number"
    assert result["value"] == "9000000009"
    assert result["use"] == "official"


def test_extract_human_name_given_is_list():
    result = extract_human_name({"family": "Smith", "given": ["John", "James"], "text": "John James Smith", "use": "official"})
    assert result["family"] == "Smith"
    assert result["given"] == ["John", "James"]
    assert result["text"] == "John James Smith"


def test_extract_human_name_missing_given_returns_empty_list():
    result = extract_human_name({"family": "Smith"})
    assert result["given"] == []


def test_extract_address():
    result = extract_address({"use": "home", "line": ["10 Victoria St"], "city": "London", "postalCode": "SW1A 1AA", "country": "GB"})
    assert result["city"] == "London"
    assert result["postalCode"] == "SW1A 1AA"
    assert result["line"] == ["10 Victoria St"]


def test_extract_contact_point():
    result = extract_contact_point({"system": "phone", "value": "020-1234-5678", "use": "home"})
    assert result["system"] == "phone"
    assert result["value"] == "020-1234-5678"


def test_extract_quantity():
    result = extract_quantity({"value": 70.5, "unit": "kg", "system": "http://unitsofmeasure.org", "code": "kg"})
    assert result["value"] == 70.5
    assert result["unit"] == "kg"


def test_extract_quantity_none_returns_none():
    assert extract_quantity(None) is None
    assert extract_quantity({}) is None


def test_extract_dosage_full():
    obj = {
        "text": "1 tablet daily",
        "timing": {"code": {"coding": [{"code": "QD"}]}},
        "doseAndRate": [{"doseQuantity": {"value": 1.0, "unit": "tablet"}}],
        "route": {"coding": [{"code": "26643006"}], "text": "Oral"},
        "site": {"text": "Mouth"},
    }
    result = extract_dosage(obj)
    assert result["text"] == "1 tablet daily"
    assert result["timing_code"] == "QD"
    assert result["dose_value"] == 1.0
    assert result["dose_unit"] == "tablet"
    assert result["route"]["text"] == "Oral"


def test_extract_dosage_empty():
    result = extract_dosage({})
    assert result["text"] is None
    assert result["timing_code"] is None
    assert result["dose_value"] is None


def test_safe_navigates_nested_dicts():
    d = {"a": {"b": {"c": "found"}}}
    assert _safe(d, "a", "b", "c") == "found"
    assert _safe(d, "a", "x", "c") is None
    assert _safe(None, "a") is None
