"""Static schemas, metadata and record mappers for the Adobe Analytics metadata tables."""

import json
from datetime import datetime, timezone
from typing import Any

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Table names and endpoints
# ---------------------------------------------------------------------------

REPORT_SUITES = "report_suites"
VIRTUAL_REPORT_SUITES = "virtual_report_suites"
DIMENSIONS = "dimensions"
METRICS = "metrics"
SEGMENTS = "segments"
CALCULATED_METRICS = "calculated_metrics"
DATE_RANGES = "date_ranges"

METADATA_TABLES = [
    REPORT_SUITES,
    VIRTUAL_REPORT_SUITES,
    DIMENSIONS,
    METRICS,
    SEGMENTS,
    CALCULATED_METRICS,
    DATE_RANGES,
]

# Paths are relative to https://analytics.adobe.io/api/{globalCompanyId}.
SUITES_PATH = "/reportsuites/collections/suites"
VRS_PATH = "/reportsuites/virtualreportsuites"
DIMENSIONS_PATH = "/dimensions"
METRICS_PATH = "/metrics"
SEGMENTS_PATH = "/segments"
CALCULATED_METRICS_PATH = "/calculatedmetrics"
DATE_RANGES_PATH = "/dateranges"
REPORTS_PATH = "/reports"

# Expansion lists. Several names are unverified against a live account, so every
# list request falls back to no expansion when Adobe answers 400.
SUITES_EXPANSION = "name,parentRsid,currency,calendarType,timezoneZoneinfo"
VRS_EXPANSION = "parentRsid,timezoneZoneinfo,description,currency,segmentList,modified"
DIMENSIONS_EXPANSION = "allowedForReporting,tags,categories"
METRICS_EXPANSION = "allowedForReporting,tags,categories"
# "compatibility" is deliberately left out: large and not useful for analytics.
SEGMENTS_EXPANSION = (
    "reportSuiteName,ownerFullName,modified,tags,definition,"
    "publishingStatus,definitionLastModified,categories"
)
CALCULATED_METRICS_EXPANSION = "reportSuiteName,ownerFullName,modified,tags,definition,categories"
DATE_RANGES_EXPANSION = "definition,ownerFullName,modified,tags,shares,usageSummary"

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

_OWNER_FIELDS = [
    StructField("owner_id", LongType(), True),
    StructField("owner_name", StringType(), True),
    StructField("owner_login", StringType(), True),
]

REPORT_SUITES_SCHEMA = StructType(
    [
        StructField("rsid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("parent_rsid", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("timezone_zoneinfo", StringType(), True),
        StructField(
            "calendar_type",
            StructType(
                [
                    StructField("rsid", StringType(), True),
                    StructField("anchor_date", StringType(), True),
                    StructField("type", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

VIRTUAL_REPORT_SUITES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("parent_rsid", StringType(), True),
        StructField("timezone_zoneinfo", StringType(), True),
        StructField("description", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("segment_list", ArrayType(StringType()), True),
        StructField("modified", TimestampType(), True),
        StructField("raw", StringType(), True),
    ]
)

DIMENSIONS_SCHEMA = StructType(
    [
        StructField("rsid", StringType(), False),
        StructField("id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("category", StringType(), True),
        StructField("extra_title_info", StringType(), True),
        StructField("description", StringType(), True),
        StructField("parent", StringType(), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("support", ArrayType(StringType()), True),
        StructField("reportable", ArrayType(StringType()), True),
        StructField("pathable", BooleanType(), True),
        StructField("segmentable", BooleanType(), True),
        StructField("allowed_for_reporting", BooleanType(), True),
        StructField("attribution_model", StringType(), True),
        StructField(
            "none_settings",
            StructType(
                [
                    StructField("include_none_by_default", BooleanType(), True),
                    StructField("none_changeable", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField("tags", StringType(), True),
    ]
)

METRICS_SCHEMA = StructType(
    [
        StructField("rsid", StringType(), False),
        StructField("id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("extra_title_info", StringType(), True),
        StructField("category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("polarity", StringType(), True),
        StructField("help_link", StringType(), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("support", ArrayType(StringType()), True),
        StructField("allocation", BooleanType(), True),
        StructField("calculated", BooleanType(), True),
        StructField("segmentable", BooleanType(), True),
        StructField("allowed_for_reporting", BooleanType(), True),
        StructField("precision", LongType(), True),
        StructField("tags", StringType(), True),
    ]
)

SEGMENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("rsid", StringType(), True),
        StructField("report_suite_name", StringType(), True),
        *_OWNER_FIELDS,
        StructField("definition", StringType(), True),
        StructField("definition_last_modified", TimestampType(), True),
        StructField("modified", TimestampType(), True),
        StructField("created", TimestampType(), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("tags", StringType(), True),
        StructField("publishing_status", StringType(), True),
    ]
)

CALCULATED_METRICS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("rsid", StringType(), True),
        StructField("report_suite_name", StringType(), True),
        *_OWNER_FIELDS,
        StructField("polarity", StringType(), True),
        StructField("precision", LongType(), True),
        StructField("type", StringType(), True),
        StructField("template", BooleanType(), True),
        StructField("definition", StringType(), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("tags", StringType(), True),
        StructField("modified", TimestampType(), True),
        StructField("created", TimestampType(), True),
    ]
)

DATE_RANGES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("definition", StringType(), True),
        StructField("owner_id", LongType(), True),
        StructField("company_id", LongType(), True),
        StructField("ims_org_id", StringType(), True),
        StructField("template", BooleanType(), True),
        StructField("curated_item", BooleanType(), True),
        StructField("system_user_owned", BooleanType(), True),
        StructField("approved", BooleanType(), True),
        StructField("favorite", BooleanType(), True),
        StructField("modified", TimestampType(), True),
        StructField("create_date", TimestampType(), True),
        StructField("disabled_date", TimestampType(), True),
        StructField("tags", StringType(), True),
        StructField("shares", StringType(), True),
        StructField("usage_summary", StringType(), True),
        StructField("alternate_variable_names", StringType(), True),
        StructField("raw", StringType(), True),
    ]
)

TABLE_SCHEMAS: dict[str, StructType] = {
    REPORT_SUITES: REPORT_SUITES_SCHEMA,
    VIRTUAL_REPORT_SUITES: VIRTUAL_REPORT_SUITES_SCHEMA,
    DIMENSIONS: DIMENSIONS_SCHEMA,
    METRICS: METRICS_SCHEMA,
    SEGMENTS: SEGMENTS_SCHEMA,
    CALCULATED_METRICS: CALCULATED_METRICS_SCHEMA,
    DATE_RANGES: DATE_RANGES_SCHEMA,
}

TABLE_PRIMARY_KEYS: dict[str, list[str]] = {
    REPORT_SUITES: ["rsid"],
    VIRTUAL_REPORT_SUITES: ["id"],
    DIMENSIONS: ["rsid", "id"],
    METRICS: ["rsid", "id"],
    SEGMENTS: ["id"],
    CALCULATED_METRICS: ["id"],
    DATE_RANGES: ["id"],
}

# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------


def to_json_text(value: Any) -> str | None:
    """Store variable-shape objects as JSON text so the table schema never drifts."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True)


def to_str_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        value = [value]
    out = []
    for item in value:
        if item is None:
            continue
        if isinstance(item, dict):
            out.append(str(item.get("id")) if "id" in item else json.dumps(item, sort_keys=True))
        else:
            out.append(str(item))
    return out


def to_long(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_iso_timestamp(value: Any) -> str | None:
    """Normalize Adobe timestamps to ISO 8601; unparseable text becomes None."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Epoch values above ~1e11 can only be milliseconds.
        seconds = value / 1000.0 if value > 1e11 else float(value)
        return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    # Python < 3.11 rejects offsets without a colon (e.g. +0000).
    if len(text) > 5 and text[-5] in "+-" and text[-4:].isdigit() and ":" not in text[-5:]:
        text = text[:-2] + ":" + text[-2:]
    try:
        return datetime.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _owner_fields(item: dict) -> dict:
    owner = item.get("owner")
    if not isinstance(owner, dict):
        return {"owner_id": None, "owner_name": None, "owner_login": None}
    return {
        "owner_id": to_long(owner.get("id")),
        "owner_name": owner.get("name"),
        "owner_login": owner.get("login"),
    }


# ---------------------------------------------------------------------------
# Record mappers (API camelCase -> table snake_case)
# ---------------------------------------------------------------------------


def map_report_suite(item: dict) -> dict:
    calendar = item.get("calendarType")
    calendar_row = None
    if isinstance(calendar, dict):
        calendar_row = {
            "rsid": calendar.get("rsid"),
            "anchor_date": calendar.get("anchorDate"),
            "type": calendar.get("type"),
        }
    return {
        "rsid": item.get("rsid") or item.get("id"),
        "name": item.get("name"),
        "parent_rsid": item.get("parentRsid"),
        "currency": item.get("currency"),
        "timezone_zoneinfo": item.get("timezoneZoneinfo"),
        "calendar_type": calendar_row,
    }


def map_virtual_report_suite(item: dict) -> dict:
    return {
        "id": item.get("id") or item.get("rsid"),
        "name": item.get("name"),
        "parent_rsid": item.get("parentRsid"),
        # Adobe's VRS reference spells this both ways.
        "timezone_zoneinfo": item.get("timezoneZoneinfo") or item.get("timezoneZoneInfo"),
        "description": item.get("description"),
        "currency": item.get("currency"),
        "segment_list": to_str_list(item.get("segmentList")),
        "modified": to_iso_timestamp(item.get("modified")),
        "raw": json.dumps(item, sort_keys=True),
    }


def map_dimension(item: dict, rsid: str) -> dict:
    none_settings = item.get("noneSettings")
    none_row = None
    if isinstance(none_settings, dict):
        none_row = {
            "include_none_by_default": none_settings.get("includeNoneByDefault"),
            "none_changeable": none_settings.get("noneChangeable"),
        }
    return {
        "rsid": rsid,
        "id": item.get("id"),
        "title": item.get("title"),
        "name": item.get("name"),
        "type": item.get("type"),
        "category": item.get("category"),
        "extra_title_info": item.get("extraTitleInfo"),
        "description": item.get("description"),
        "parent": item.get("parent"),
        "categories": to_str_list(item.get("categories")),
        "support": to_str_list(item.get("support")),
        "reportable": to_str_list(item.get("reportable")),
        "pathable": item.get("pathable"),
        "segmentable": item.get("segmentable"),
        "allowed_for_reporting": item.get("allowedForReporting"),
        "attribution_model": to_json_text(item.get("attributionModel")),
        "none_settings": none_row,
        "tags": to_json_text(item.get("tags")),
    }


def map_metric(item: dict, rsid: str) -> dict:
    return {
        "rsid": rsid,
        "id": item.get("id"),
        "title": item.get("title"),
        "name": item.get("name"),
        "type": item.get("type"),
        "extra_title_info": item.get("extraTitleInfo"),
        "category": item.get("category"),
        "description": item.get("description"),
        "polarity": item.get("polarity"),
        "help_link": item.get("helpLink"),
        "categories": to_str_list(item.get("categories")),
        "support": to_str_list(item.get("support")),
        "allocation": item.get("allocation"),
        "calculated": item.get("calculated"),
        "segmentable": item.get("segmentable"),
        "allowed_for_reporting": item.get("allowedForReporting"),
        "precision": to_long(item.get("precision")),
        "tags": to_json_text(item.get("tags")),
    }


def map_segment(item: dict) -> dict:
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "description": item.get("description"),
        "rsid": item.get("rsid"),
        "report_suite_name": item.get("reportSuiteName"),
        **_owner_fields(item),
        "definition": to_json_text(item.get("definition")),
        "definition_last_modified": to_iso_timestamp(item.get("definitionLastModified")),
        "modified": to_iso_timestamp(item.get("modified")),
        "created": to_iso_timestamp(item.get("created")),
        "categories": to_str_list(item.get("categories")),
        "tags": to_json_text(item.get("tags")),
        "publishing_status": to_json_text(item.get("publishingStatus")),
    }


def map_calculated_metric(item: dict) -> dict:
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "description": item.get("description"),
        "rsid": item.get("rsid"),
        "report_suite_name": item.get("reportSuiteName"),
        **_owner_fields(item),
        "polarity": item.get("polarity"),
        "precision": to_long(item.get("precision")),
        "type": item.get("type"),
        "template": item.get("template"),
        "definition": to_json_text(item.get("definition")),
        "categories": to_str_list(item.get("categories")),
        "tags": to_json_text(item.get("tags")),
        "modified": to_iso_timestamp(item.get("modified")),
        "created": to_iso_timestamp(item.get("created")),
    }


def map_date_range(item: dict) -> dict:
    owner = item.get("owner")
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "description": item.get("description"),
        "definition": to_json_text(item.get("definition")),
        "owner_id": to_long(owner.get("id")) if isinstance(owner, dict) else None,
        "company_id": to_long(item.get("companyId")),
        "ims_org_id": item.get("imsOrgId"),
        "template": item.get("template"),
        "curated_item": item.get("curatedItem"),
        "system_user_owned": item.get("systemUserOwned"),
        "approved": item.get("approved"),
        "favorite": item.get("favorite"),
        "modified": to_iso_timestamp(item.get("modified")),
        "create_date": to_iso_timestamp(item.get("createDate")),
        "disabled_date": to_iso_timestamp(item.get("disabledDate")),
        "tags": to_json_text(item.get("tags")),
        "shares": to_json_text(item.get("shares")),
        "usage_summary": to_json_text(item.get("usageSummary")),
        "alternate_variable_names": to_json_text(item.get("alternateVariableNames")),
        "raw": json.dumps(item, sort_keys=True),
    }
