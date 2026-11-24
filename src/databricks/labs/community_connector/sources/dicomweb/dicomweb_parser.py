"""
DICOM JSON tag parser for QIDO-RS responses.

QIDO-RS returns arrays of DICOM JSON objects keyed by 8-digit uppercase hex tags:
    {
        "00080020": {"vr": "DA", "Value": ["20231215"]},
        "00100010": {"vr": "PN", "Value": [{"Alphabetic": "Doe^John"}]}
    }

This module maps those tags to human-readable field names and extracts values
with awareness of DICOM Value Representations (VR).
"""

# ---------------------------------------------------------------------------
# Tag maps
# ---------------------------------------------------------------------------

STUDY_TAG_MAP: dict[str, str] = {
    "0020000D": "study_instance_uid",
    "00080020": "study_date",
    "00080030": "study_time",
    "00080050": "accession_number",
    "00100010": "patient_name",
    "00100020": "patient_id",
    "00081030": "study_description",
    "00080061": "modalities_in_study",
    "00201206": "number_of_study_related_series",
    "00201208": "number_of_study_related_instances",
}

SERIES_TAG_MAP: dict[str, str] = {
    "0020000E": "series_instance_uid",
    "0020000D": "study_instance_uid",
    "00080020": "study_date",
    "00200011": "series_number",
    "0008103E": "series_description",
    "00080060": "modality",
    "00180015": "body_part_examined",
    "00080021": "series_date",
}

INSTANCE_TAG_MAP: dict[str, str] = {
    "00080018": "sop_instance_uid",
    "0020000E": "series_instance_uid",
    "0020000D": "study_instance_uid",
    "00080016": "sop_class_uid",
    "00200013": "instance_number",
    "00080020": "study_date",
    "00080023": "content_date",
    "00080033": "content_time",
}

# VRs that carry a single string scalar (or list → join / first element)
_STRING_VRS = {"DA", "TM", "CS", "LO", "UI", "SH", "LT", "ST", "UT", "AE", "AS", "DT", "UC", "UR"}
# VRs that carry numeric values
_NUMERIC_VRS = {
    "IS",
    "DS",
    "FL",
    "FD",
    "SL",
    "SS",
    "UL",
    "US",
    "AT",
    "OB",
    "OW",
    "OF",
    "OD",
    "OL",
    "OV",
}
# Multi-valued string VRs (arrays stay as lists)
_MULTI_STRING_VRS = {"CS"}  # modalities_in_study uses CS and can be multi-valued


# ---------------------------------------------------------------------------
# Low-level value extraction
# ---------------------------------------------------------------------------


def _extract_pn_value(first: object) -> object:
    """Extract a Person Name value from the first element."""
    if isinstance(first, dict):
        return first.get("Alphabetic") or first.get("Ideographic") or first.get("Phonetic")
    return str(first)


def _extract_string_value(values: list, field_name: str) -> object:
    """Extract a string-type value, handling multi-valued fields."""
    if field_name == "modalities_in_study":
        return [str(v) for v in values]
    return str(values[0])


def _extract_numeric_value(first: object, vr: str) -> object:
    """Extract a numeric value with appropriate int/float conversion."""
    try:
        if vr in {"IS", "US", "UL", "SS", "SL"}:
            return int(first)
        return float(first)
    except (TypeError, ValueError):
        return None


def _extract_value(tag_obj: dict, field_name: str) -> object:
    """Extract a Python value from a DICOM JSON tag object."""
    vr = tag_obj.get("vr", "")
    values = tag_obj.get("Value", [])

    if not values:
        return None
    if vr == "PN":
        return _extract_pn_value(values[0])
    if vr in _STRING_VRS:
        return _extract_string_value(values, field_name)
    if vr in _NUMERIC_VRS:
        return _extract_numeric_value(values[0], vr)
    # Fallback: return first value as-is or stringified
    first = values[0]
    return first if isinstance(first, (str, int, float, bool)) else str(first)


# ---------------------------------------------------------------------------
# Public parse functions
# ---------------------------------------------------------------------------


def parse_dicom_json(dicom_obj: dict, tag_map: dict[str, str]) -> dict:
    """
    Convert a single DICOM JSON object to a Python dict using the given tag map.

    Args:
        dicom_obj: Dict keyed by 8-character uppercase hex tags.
        tag_map:   Mapping of hex tag → field name.

    Returns:
        Dict of {field_name: value} for all recognised tags that have a value.
    """
    result: dict = {}
    for tag, field_name in tag_map.items():
        tag_upper = tag.upper()
        tag_obj = dicom_obj.get(tag_upper) or dicom_obj.get(tag.lower())
        if tag_obj is None:
            result[field_name] = None
            continue
        result[field_name] = _extract_value(tag_obj, field_name)
    return result


def parse_study(dicom_obj: dict) -> dict:
    """Parse a QIDO-RS study-level DICOM JSON object."""
    return parse_dicom_json(dicom_obj, STUDY_TAG_MAP)


def parse_series(dicom_obj: dict) -> dict:
    """Parse a QIDO-RS series-level DICOM JSON object."""
    return parse_dicom_json(dicom_obj, SERIES_TAG_MAP)


def parse_instance(dicom_obj: dict) -> dict:
    """Parse a QIDO-RS instance-level DICOM JSON object."""
    record = parse_dicom_json(dicom_obj, INSTANCE_TAG_MAP)
    # dicom_file_path is filled in later by the connector when WADO-RS retrieval is enabled
    record.setdefault("dicom_file_path", None)
    # metadata is filled in later when fetch_metadata=true
    record.setdefault("metadata", None)
    return record
