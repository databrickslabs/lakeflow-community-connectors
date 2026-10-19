"""Lakeflow connector for daily Google Campaign Manager 360 ad reports."""

import calendar
import csv
import io
import json
import logging
import math
import re
import time
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from dataclasses import replace as dataclass_replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from typing import Iterator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.google_cm360.google_cm360_schemas import (
    AD_REPORT_DAILY_METADATA,
    AD_REPORT_DAILY_SCHEMA,
    AD_REPORT_PLACEMENT_DAILY_METADATA,
    AD_REPORT_PLACEMENT_DAILY_SCHEMA,
    CAMPAIGN_PERIOD_FIELD_SPECS,
    DIMENSION_API_NAMES,
    FIELD_SPECS,
    METRIC_API_NAMES,
    PERIOD_REACH_FIELD_SPECS,
    PLACEMENT_DIMENSION_API_NAMES,
    PLACEMENT_FIELD_SPECS,
    PLACEMENT_METRIC_API_NAMES,
    REACH_DIMENSION_API_NAMES,
    REACH_FIELD_SPECS,
    REACH_METRIC_API_NAMES,
    REACH_REPORT_DAILY_METADATA,
    REACH_REPORT_DAILY_SCHEMA,
    REACH_REPORT_PERIOD_METADATA,
    REACH_REPORT_PERIOD_SCHEMA,
)

AD_REPORT_TABLE_NAME = "ad_report_daily"
REACH_REPORT_TABLE_NAME = "reach_report_daily"
PLACEMENT_REPORT_TABLE_NAME = "ad_report_placement_daily"
PERIOD_REACH_TABLE_NAME = "reach_report_period"
_API_BASE_URL = "https://dfareporting.googleapis.com/dfareporting/v5"
_RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_MAX_HTTP_ATTEMPTS = 5
_HTTP_TIMEOUT_SECONDS = 30
_MAX_RETRY_DELAY_SECONDS = 30.0
_MAX_FILE_POLLS = 60
_CSV_FOOTER_LABELS = {"Grand Total:", "Grand Total", "Report Time:", "Report Time"}
_ISO_DATE_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_INTEGER_PATTERN = re.compile(r"[0-9]+")
_DECIMAL_PATTERN = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?")
_DECIMAL_QUANTUM = Decimal("0.000000001")
logger = logging.getLogger(__name__)
DELIVERY_COUNTRY_NAMES = {
    "US": "United States",
    "DE": "Germany",
    "CH": "Switzerland",
}
_TABLE_OPTION_NAMES = {
    "sync_start_date",
    "sync_end_date",
    "initial_lookback_months",
    "lookback_window_days",
    "reporting_timezone",
    "advertiser_ids",
    "delivery_country_codes",
    "max_records_per_batch",
    "emit_request_telemetry",
}
_RESPONSE_STATUS_KEYS = (
    "200",
    "202",
    "204",
    "400",
    "401",
    "403",
    "404",
    "429",
    "500",
    "502",
    "503",
    "504",
    "other",
)
_RETRY_STATUS_KEYS = ("429", "500", "502", "503", "504")


def _parse_delivery_country_codes(raw: object | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(
            'delivery_country_codes must be one of ["US"], ["DE"], or ["CH"]'
        ) from error
    if (
        not isinstance(parsed, list)
        or len(parsed) != 1
        or not isinstance(parsed[0], str)
        or parsed[0] not in DELIVERY_COUNTRY_NAMES
    ):
        raise ValueError('delivery_country_codes must be one of ["US"], ["DE"], or ["CH"]')
    return (parsed[0],)


@dataclass(frozen=True)
class ReadOptions:
    """Validated table options for one trigger."""

    sync_start_date: date | None
    sync_end_date: date | None
    initial_lookback_months: int
    lookback_window_days: int
    reporting_timezone: ZoneInfo
    advertiser_ids: tuple[str, ...]
    delivery_country_codes: tuple[str, ...]
    max_records_per_batch: int
    emit_request_telemetry: bool


@dataclass(frozen=True)
class ReadState:
    """Durable date state for an initial read or one lookback replay."""

    high_watermark: date | None
    next_date: date
    upper_bound: date
    initial_start: date


@dataclass
class RequestTelemetry:
    """Sanitized request counters optionally persisted in one table read offset."""

    daily_reports_completed: int = 0
    http_attempts: int = 0
    http_retries: int = 0
    file_poll_calls: int = 0
    transport_errors: int = 0
    response_status_counts: dict[str, int] = dataclass_field(
        default_factory=lambda: {key: 0 for key in _RESPONSE_STATUS_KEYS}
    )
    retry_status_counts: dict[str, int] = dataclass_field(
        default_factory=lambda: {key: 0 for key in _RETRY_STATUS_KEYS}
    )

    def increment(self, name: str) -> None:
        value = getattr(self, name)
        setattr(self, name, value + 1)

    def record_response(self, status_code: int) -> None:
        key = str(status_code)
        if key not in self.response_status_counts:
            key = "other"
        self.response_status_counts[key] += 1

    def record_retry(self, status_code: int) -> None:
        self.http_retries += 1
        self.retry_status_counts[str(status_code)] += 1

    def as_dict(self) -> dict:
        return {
            "daily_reports_completed": self.daily_reports_completed,
            "http_attempts": self.http_attempts,
            "http_retries": self.http_retries,
            "file_poll_calls": self.file_poll_calls,
            "transport_errors": self.transport_errors,
            "response_status_counts": dict(self.response_status_counts),
            "retry_status_counts": dict(self.retry_status_counts),
        }

    @classmethod
    def from_dict(cls, value: object) -> "RequestTelemetry":
        expected_scalar_keys = {
            "daily_reports_completed",
            "http_attempts",
            "http_retries",
            "file_poll_calls",
            "transport_errors",
        }
        expected_keys = expected_scalar_keys | {
            "response_status_counts",
            "retry_status_counts",
        }
        if not isinstance(value, dict) or set(value) != expected_keys:
            raise ValueError("request_telemetry has missing or unknown keys")
        if any(
            not isinstance(value[key], int) or isinstance(value[key], bool) or value[key] < 0
            for key in expected_scalar_keys
        ):
            raise ValueError("request_telemetry counters must be non-negative integers")
        response_counts = value["response_status_counts"]
        retry_counts = value["retry_status_counts"]
        if (
            not isinstance(response_counts, dict)
            or set(response_counts) != set(_RESPONSE_STATUS_KEYS)
            or not isinstance(retry_counts, dict)
            or set(retry_counts) != set(_RETRY_STATUS_KEYS)
        ):
            raise ValueError("request_telemetry status counters have missing or unknown keys")
        if any(
            not isinstance(counter, int) or isinstance(counter, bool) or counter < 0
            for counter in (*response_counts.values(), *retry_counts.values())
        ):
            raise ValueError("request_telemetry status counters must be non-negative integers")
        return cls(
            **{key: value[key] for key in expected_scalar_keys},
            response_status_counts=dict(response_counts),
            retry_status_counts=dict(retry_counts),
        )


@dataclass(frozen=True)
class ProfileMetadata:
    """Validated profile identity shared by every report row."""

    account_id: str
    account_name: str
    subaccount_id: str | None
    subaccount_name: str | None

    def as_row(self) -> dict[str, str | None]:
        return {
            "account_id": self.account_id,
            "account_name": self.account_name,
            "subaccount_id": self.subaccount_id,
            "subaccount_name": self.subaccount_name,
        }


class Cm360ApiError(RuntimeError):
    """A sanitized CM360 transport or response error."""


class Cm360ConfigurationError(ValueError):
    """The configured CM360 profile cannot satisfy the connector contract."""


class Cm360DataError(ValueError):
    """A CM360 report export violates the fixed connector data contract."""


class Cm360ReportError(RuntimeError):
    """A CM360 asynchronous report did not become available."""


def _required_text(resource: dict, field_name: str, resource_name: str) -> str:
    value = resource.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise Cm360ConfigurationError(f"CM360 {resource_name} response is missing {field_name}")
    return value


def _required_numeric_id(resource: dict, field_name: str, resource_name: str) -> str:
    value = resource.get(field_name)
    if not isinstance(value, str) or _INTEGER_PATTERN.fullmatch(value) is None:
        raise Cm360ConfigurationError(f"CM360 {resource_name} response has invalid {field_name}")
    return value


def _build_profile_metadata(profile: dict) -> ProfileMetadata:
    account_id = _required_numeric_id(profile, "accountId", "profile")
    raw_subaccount_id = profile.get("subAccountId")
    raw_subaccount_name = profile.get("subAccountName")
    has_subaccount_id = raw_subaccount_id not in (None, "")
    has_subaccount_name = raw_subaccount_name not in (None, "")
    if has_subaccount_id != has_subaccount_name:
        raise Cm360ConfigurationError("CM360 profile has incomplete subaccount identity")
    if has_subaccount_id:
        subaccount_id = _required_numeric_id(profile, "subAccountId", "profile")
        subaccount_name = _required_text(profile, "subAccountName", "profile")
    else:
        subaccount_id = None
        subaccount_name = None

    return ProfileMetadata(
        account_id=account_id,
        account_name=_required_text(profile, "accountName", "profile"),
        subaccount_id=subaccount_id,
        subaccount_name=subaccount_name,
    )


def _build_report_definition(
    report_date: date,
    advertiser_ids: tuple[str, ...],
    delivery_country_id: str | None = None,
) -> dict:
    """Build the exact one-day STANDARD report used for validation and insertion."""

    report_date_text = report_date.isoformat()
    criteria = {
        "dateRange": {
            "startDate": report_date_text,
            "endDate": report_date_text,
        },
        "dimensions": [{"name": name} for name in DIMENSION_API_NAMES],
        "metricNames": list(METRIC_API_NAMES),
    }
    dimension_filters = [
        {"dimensionName": "advertiser", "id": advertiser_id} for advertiser_id in advertiser_ids
    ]
    if delivery_country_id is not None:
        dimension_filters.append({"dimensionName": "country", "id": delivery_country_id})
    if dimension_filters:
        criteria["dimensionFilters"] = dimension_filters
    return {
        "name": f"lakeflow-google-cm360-{report_date_text}",
        "type": "STANDARD",
        "format": "CSV",
        "criteria": criteria,
    }


def _build_reach_report_definition(
    report_date: date,
    advertiser_ids: tuple[str, ...],
    delivery_country_id: str | None = None,
) -> dict:
    """Build the exact one-day REACH report used by the reach table."""

    report_date_text = report_date.isoformat()
    reach_criteria = {
        "dateRange": {
            "startDate": report_date_text,
            "endDate": report_date_text,
        },
        "dimensions": [{"name": name} for name in REACH_DIMENSION_API_NAMES],
        "metricNames": list(REACH_METRIC_API_NAMES),
    }
    dimension_filters = [
        {"dimensionName": "advertiser", "id": advertiser_id} for advertiser_id in advertiser_ids
    ]
    if delivery_country_id is not None:
        dimension_filters.append({"dimensionName": "country", "id": delivery_country_id})
    if dimension_filters:
        reach_criteria["dimensionFilters"] = dimension_filters
    return {
        "name": f"lakeflow-google-cm360-reach-{report_date_text}",
        "type": "REACH",
        "format": "CSV",
        "reachCriteria": reach_criteria,
    }


def _build_placement_report_definition(
    report_date: date,
    advertiser_ids: tuple[str, ...],
    delivery_country_id: str | None = None,
) -> dict:
    """Build the one-day STANDARD placement report; it has no country dimension."""

    if delivery_country_id is not None:
        raise Cm360ConfigurationError(
            f"{PLACEMENT_REPORT_TABLE_NAME} does not support a delivery-country filter"
        )
    report_date_text = report_date.isoformat()
    criteria = {
        "dateRange": {
            "startDate": report_date_text,
            "endDate": report_date_text,
        },
        "dimensions": [{"name": name} for name in PLACEMENT_DIMENSION_API_NAMES],
        "metricNames": list(PLACEMENT_METRIC_API_NAMES),
    }
    dimension_filters = [
        {"dimensionName": "advertiser", "id": advertiser_id} for advertiser_id in advertiser_ids
    ]
    if dimension_filters:
        criteria["dimensionFilters"] = dimension_filters
    return {
        "name": f"lakeflow-google-cm360-placement-{report_date_text}",
        "type": "STANDARD",
        "format": "CSV",
        "criteria": criteria,
    }


def _campaign_period(start: str | None, end: str | None, cutoff: date) -> tuple:
    """Use completed duration, never campaign age or a rolling window."""
    if start is None:
        return None, None, "UNPROVEN_START"
    lower = _parse_iso_date(start, "campaign_start_date")
    upper = min(_parse_iso_date(end, "campaign_end_date"), cutoff) if end else cutoff
    if lower > upper:
        raise Cm360DataError("Campaign period ends before its start")
    return lower, upper, "WINDOW_LIMIT" if (upper - lower).days + 1 > 92 else "COMPLETE"


def _build_period_reach_report(start: date, end: date, campaign_id: str, country_id: str) -> dict:
    return {
        "name": f"lakeflow-google-cm360-period-{campaign_id}",
        "type": "REACH",
        "format": "CSV",
        "reachCriteria": {
            "dateRange": {"startDate": start.isoformat(), "endDate": end.isoformat()},
            "dimensions": [{"name": n} for n in REACH_DIMENSION_API_NAMES if n != "date"],
            "metricNames": list(REACH_METRIC_API_NAMES),
            "dimensionFilters": [
                {"dimensionName": "campaign", "id": campaign_id},
                {"dimensionName": "country", "id": country_id},
            ],
        },
    }


@dataclass(frozen=True)
class TableContract:
    schema: StructType
    metadata: dict
    field_specs: tuple
    report_builder: object
    supports_delivery_country: bool = True


TABLE_CONTRACTS = {
    AD_REPORT_TABLE_NAME: TableContract(
        AD_REPORT_DAILY_SCHEMA,
        AD_REPORT_DAILY_METADATA,
        FIELD_SPECS,
        _build_report_definition,
    ),
    REACH_REPORT_TABLE_NAME: TableContract(
        REACH_REPORT_DAILY_SCHEMA,
        REACH_REPORT_DAILY_METADATA,
        REACH_FIELD_SPECS,
        _build_reach_report_definition,
    ),
    PLACEMENT_REPORT_TABLE_NAME: TableContract(
        AD_REPORT_PLACEMENT_DAILY_SCHEMA,
        AD_REPORT_PLACEMENT_DAILY_METADATA,
        PLACEMENT_FIELD_SPECS,
        _build_placement_report_definition,
        supports_delivery_country=False,
    ),
    PERIOD_REACH_TABLE_NAME: TableContract(
        REACH_REPORT_PERIOD_SCHEMA,
        REACH_REPORT_PERIOD_METADATA,
        PERIOD_REACH_FIELD_SPECS,
        _build_period_reach_report,
    ),
}


def _retry_delay(response: requests.Response, attempt: int) -> float:
    """Return a Retry-After or exponential delay, always bounded to 30 seconds."""

    fallback = min(float(2**attempt), _MAX_RETRY_DELAY_SECONDS)
    header = response.headers.get("Retry-After")
    if header is None:
        return fallback

    try:
        delay = float(header)
        if not math.isfinite(delay) or delay < 0:
            return fallback
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(header)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
            if delay < 0:
                return fallback
        except (TypeError, ValueError, OverflowError):
            return fallback
    return min(max(delay, 0.0), _MAX_RETRY_DELAY_SECONDS)


class Cm360Client:
    """Minimal direct REST client for the CM360 Reporting API."""

    def __init__(self, access_token: str, profile_id: str) -> None:
        self._profile_id = profile_id
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            }
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
        expected_statuses: tuple[int, ...] = (200,),
        telemetry: RequestTelemetry | None = None,
    ) -> requests.Response:
        """Issue a bounded request without exposing response bodies or credentials."""

        url = f"{_API_BASE_URL}{path}"
        for attempt in range(_MAX_HTTP_ATTEMPTS):
            if telemetry is not None:
                telemetry.increment("http_attempts")
            try:
                response = self._session.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    timeout=_HTTP_TIMEOUT_SECONDS,
                )
            except requests.RequestException as error:
                if telemetry is not None:
                    telemetry.increment("transport_errors")
                raise Cm360ApiError(
                    f"{method} CM360 API request failed on attempt {attempt + 1}"
                ) from error

            if telemetry is not None:
                telemetry.record_response(response.status_code)

            if response.status_code in expected_statuses:
                return response
            if response.status_code in _RETRIABLE_STATUS_CODES and attempt < _MAX_HTTP_ATTEMPTS - 1:
                if telemetry is not None:
                    telemetry.record_retry(response.status_code)
                time.sleep(_retry_delay(response, attempt))
                continue

            attempt_word = "attempt" if attempt == 0 else "attempts"
            raise Cm360ApiError(
                f"{method} CM360 API returned HTTP {response.status_code} "
                f"after {attempt + 1} {attempt_word}"
            )

        raise AssertionError("unreachable retry state")

    @staticmethod
    def _json_object(response: requests.Response, operation: str) -> dict:
        try:
            value = response.json()
        except (requests.JSONDecodeError, ValueError) as error:
            raise Cm360ApiError(f"CM360 {operation} returned invalid JSON") from error
        if not isinstance(value, dict):
            raise Cm360ApiError(f"CM360 {operation} returned invalid JSON object")
        return value

    def get_profile(self, *, telemetry: RequestTelemetry | None = None) -> dict:
        response = self._request("GET", f"/userprofiles/{self._profile_id}", telemetry=telemetry)
        profile = self._json_object(response, "profile check")
        profile_id = _required_numeric_id(profile, "profileId", "profile")
        if profile_id != self._profile_id:
            raise Cm360ConfigurationError(
                "CM360 profile response does not match the configured profile_id"
            )
        return profile

    def query_dimension_values(
        self,
        dimension_name: str,
        exact_value: str,
        start_date: date,
        end_date: date,
        *,
        telemetry: RequestTelemetry | None = None,
    ) -> dict:
        matches = []
        page_token = None
        seen_page_tokens = set()
        while True:
            params = {"maxResults": 100}
            if page_token is not None:
                params["pageToken"] = page_token
            response = self._request(
                "POST",
                f"/userprofiles/{self._profile_id}/dimensionvalues/query",
                params=params,
                json_body={
                    "dimensionName": dimension_name,
                    "filters": [{"dimensionName": dimension_name, "value": exact_value}],
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                },
                telemetry=telemetry,
            )
            page = self._json_object(response, "dimension-values query")
            matches.extend(
                item
                for item in page.get("items", [])
                if isinstance(item, dict)
                and item.get("dimensionName") == dimension_name
                and item.get("value") == exact_value
                and item.get("matchType", "EXACT") == "EXACT"
            )
            if matches:
                return {"items": matches}
            next_page_token = page.get("nextPageToken")
            if next_page_token in (None, ""):
                return {"items": matches}
            if not isinstance(next_page_token, str) or next_page_token in seen_page_tokens:
                raise Cm360ApiError("CM360 dimension-values query returned invalid pagination")
            seen_page_tokens.add(next_page_token)
            page_token = next_page_token

    def query_compatible_fields(
        self, report: dict, *, telemetry: RequestTelemetry | None = None
    ) -> dict:
        response = self._request(
            "POST",
            f"/userprofiles/{self._profile_id}/reports/compatiblefields/query",
            json_body=report,
            telemetry=telemetry,
        )
        return self._json_object(response, "compatible-fields query")

    def validate_compatible_fields(
        self, report: dict, *, telemetry: RequestTelemetry | None = None
    ) -> None:
        self.query_compatible_fields(report, telemetry=telemetry)

    def insert_report(self, report: dict, *, telemetry: RequestTelemetry | None = None) -> str:
        response = self._request(
            "POST",
            f"/userprofiles/{self._profile_id}/reports",
            json_body=report,
            telemetry=telemetry,
        )
        report_id = self._json_object(response, "report insertion").get("id")
        if report_id is None or str(report_id) == "":
            raise Cm360ApiError("CM360 report insertion returned no report ID")
        return str(report_id)

    def run_report(self, report_id: str, *, telemetry: RequestTelemetry | None = None) -> str:
        response = self._request(
            "POST",
            f"/userprofiles/{self._profile_id}/reports/{report_id}/run",
            params={"synchronous": "false"},
            telemetry=telemetry,
        )
        file_id = self._json_object(response, "report run").get("id")
        if file_id is None or str(file_id) == "":
            raise Cm360ApiError("CM360 report run returned no file ID")
        return str(file_id)

    def get_file(
        self,
        report_id: str,
        file_id: str,
        *,
        telemetry: RequestTelemetry | None = None,
    ) -> dict:
        if telemetry is not None:
            telemetry.increment("file_poll_calls")
        response = self._request(
            "GET",
            f"/userprofiles/{self._profile_id}/reports/{report_id}/files/{file_id}",
            telemetry=telemetry,
        )
        return self._json_object(response, "file poll")

    def download_file(
        self,
        report_id: str,
        file_id: str,
        *,
        telemetry: RequestTelemetry | None = None,
    ) -> str:
        response = self._request(
            "GET",
            f"/userprofiles/{self._profile_id}/reports/{report_id}/files/{file_id}",
            params={"alt": "media"},
            telemetry=telemetry,
        )
        return response.text

    def delete_report(self, report_id: str, *, telemetry: RequestTelemetry | None = None) -> None:
        self._request(
            "DELETE",
            f"/userprofiles/{self._profile_id}/reports/{report_id}",
            expected_statuses=(200, 204),
            telemetry=telemetry,
        )


def _parse_file_timestamp(value: str) -> str:
    if not isinstance(value, str) or _INTEGER_PATTERN.fullmatch(value) is None:
        raise Cm360DataError("File lastModifiedTime is not valid epoch milliseconds")
    try:
        parsed = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=int(value))
    except (OverflowError, ValueError) as error:
        raise Cm360DataError("File lastModifiedTime is not valid epoch milliseconds") from error
    return parsed.isoformat()


def _parse_csv_value(field, value: str):
    if field.kind == "native_string":
        return value
    if field.kind == "integer":
        if value == "":
            return None
        if _INTEGER_PATTERN.fullmatch(value) is None:
            raise Cm360DataError(f"Invalid {field.output_name} integer value")
        return int(value)

    if field.kind == "decimal":
        if value == "":
            return None
        if _DECIMAL_PATTERN.fullmatch(value) is None:
            raise Cm360DataError(f"Invalid {field.output_name} decimal value")
        try:
            return format(Decimal(value).quantize(_DECIMAL_QUANTUM), "f")
        except InvalidOperation as error:
            raise Cm360DataError(f"Invalid {field.output_name} decimal value") from error

    if field.kind == "date":
        if _ISO_DATE_PATTERN.fullmatch(value) is None:
            raise Cm360DataError(f"Invalid {field.output_name} date value")
        try:
            return date.fromisoformat(value).isoformat()
        except ValueError as error:
            raise Cm360DataError(f"Invalid {field.output_name} date value") from error

    if field.output_name == "delivery_country":
        value = value.strip()
    if field.output_name in {"advertiser_id", "campaign_id", "placement_id"} and (
        _INTEGER_PATTERN.fullmatch(value) is None
    ):
        raise Cm360DataError(f"{field.output_name} must be a numeric CM360 ID")
    if field.output_name in {"delivery_country", "advertiser_name"} and not value.strip():
        raise Cm360DataError(f"{field.output_name} must be non-empty")
    return value or None


def _report_csv_rows(csv_text: str, field_specs: tuple) -> tuple[list[str], list[list[str]]]:
    """Locate and validate the source header before parsing data rows."""
    csv_rows = list(csv.reader(io.StringIO(csv_text)))
    marker_index = next(
        (index for index, row in enumerate(csv_rows) if row == ["Report Fields"]),
        None,
    )
    if marker_index is None:
        raise Cm360DataError("CM360 CSV is missing the Report Fields marker")

    header_index = next(
        (
            index
            for index in range(marker_index + 1, len(csv_rows))
            if any(cell != "" for cell in csv_rows[index])
        ),
        None,
    )
    if header_index is None:
        raise Cm360DataError("CM360 CSV is missing its report header")

    expected_header = [field.csv_header for field in field_specs]
    header = csv_rows[header_index]
    if len(header) != len(set(header)):
        raise Cm360DataError("CM360 CSV header contains duplicate fields")
    missing_headers = sorted(set(expected_header) - set(header))
    unexpected_headers = sorted(set(header) - set(expected_header))
    if missing_headers or unexpected_headers:
        missing = ", ".join(missing_headers) or "none"
        unexpected = ", ".join(unexpected_headers) or "none"
        raise Cm360DataError(
            f"CM360 CSV header does not match the configured fields "
            f"(missing: {missing}; unexpected: {unexpected})"
        )
    return header, csv_rows[header_index + 1 :]


def _parse_report_csv(
    csv_text: str,
    *,
    profile_id: str,
    expected_report_date: date,
    profile_metadata: ProfileMetadata,
    file_last_modified_at: str,
    expected_delivery_country: str | None = None,
    field_specs: tuple = FIELD_SPECS,
    key_fields: tuple = ("delivery_country", "advertiser_id", "campaign_id"),
) -> list[dict]:
    """Parse a complete CM360 CSV without guessing headers or numeric formats."""

    parsed_file_timestamp = _parse_file_timestamp(file_last_modified_at)
    header, data_rows = _report_csv_rows(csv_text, field_specs)
    fields_by_header = {field.csv_header: field for field in field_specs}
    identifier_indexes = [header.index("Campaign ID")]

    output = []
    seen_primary_keys = set()
    footer_started = False
    native_footer_seen = False
    for row in data_rows:
        if not any(cell != "" for cell in row):
            continue
        first_cell = row[0].strip() if row else ""
        has_numeric_identifier = any(
            index < len(row) and _INTEGER_PATTERN.fullmatch(row[index].strip()) is not None
            for index in identifier_indexes
        )
        if (
            first_cell in _CSV_FOOTER_LABELS
            and len(row) <= len(header)
            and not has_numeric_identifier
        ):
            footer_started = True
            native_footer_seen = native_footer_seen or first_cell == "Grand Total:"
            continue
        if footer_started:
            if has_numeric_identifier:
                raise Cm360DataError("CM360 CSV contains data after its footer")
            continue
        if len(row) != len(header):
            raise Cm360DataError("CM360 CSV data row has an invalid column count")

        parsed_row = {
            fields_by_header[csv_header].output_name: _parse_csv_value(
                fields_by_header[csv_header], value
            )
            for csv_header, value in zip(header, row)
        }
        parsed_report_date = parsed_row.pop("report_date", expected_report_date.isoformat())
        if parsed_report_date != expected_report_date.isoformat():
            raise Cm360DataError("CM360 CSV Date does not match the requested report date")
        if (
            expected_delivery_country is not None
            and parsed_row["delivery_country"] != expected_delivery_country
        ):
            raise Cm360DataError("CM360 CSV row does not match configured delivery country")
        primary_key = (
            profile_id,
            expected_report_date.isoformat(),
            *(parsed_row[key] for key in key_fields),
        )
        if primary_key in seen_primary_keys:
            raise Cm360DataError("CM360 CSV contains a duplicate primary key")
        seen_primary_keys.add(primary_key)
        output.append(
            {
                "profile_id": profile_id,
                **profile_metadata.as_row(),
                "report_date": expected_report_date.isoformat(),
                **parsed_row,
                "report_file_last_modified_at": parsed_file_timestamp,
            }
        )
    if not native_footer_seen:
        raise Cm360DataError("CM360 CSV is incomplete: missing native footer")
    return output


def _telemetry_kwargs(telemetry: RequestTelemetry | None) -> dict:
    return {} if telemetry is None else {"telemetry": telemetry}


def _poll_until_available(
    client: Cm360Client,
    report_id: str,
    file_id: str,
    telemetry: RequestTelemetry | None = None,
) -> dict:
    """Poll a CM360 File resource with a separate bounded backoff budget."""

    for attempt in range(_MAX_FILE_POLLS):
        file_resource = client.get_file(report_id, file_id, **_telemetry_kwargs(telemetry))
        status = file_resource.get("status")
        if status == "REPORT_AVAILABLE":
            return file_resource
        if status in {"FAILED", "CANCELLED"}:
            raise Cm360ReportError(f"CM360 report file entered terminal state {status}")
        if attempt < _MAX_FILE_POLLS - 1:
            time.sleep(min(2 * 1.5**attempt, _MAX_RETRY_DELAY_SECONDS))
    raise Cm360ReportError(f"CM360 report file was unavailable after {_MAX_FILE_POLLS} polls")


def _parse_iso_date(value: object, name: str) -> date:
    if not isinstance(value, str) or _ISO_DATE_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a non-empty ISO date (YYYY-MM-DD)")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an ISO date (YYYY-MM-DD)") from error


def _parse_positive_int(options: dict[str, str], name: str, default: int) -> int:
    raw_value = options.get(name, str(default))
    try:
        value = int(raw_value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a positive integer") from error
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _parse_strict_bool(options: dict[str, str], name: str, default: bool) -> bool:
    raw_value = options.get(name, "true" if default else "false")
    if raw_value not in {"true", "false"}:
        raise ValueError(f"{name} must be a strict boolean: true or false")
    return raw_value == "true"


def _parse_read_options(table_options: dict[str, str]) -> ReadOptions:
    """Validate the public string-encoded table options."""

    table_options = {
        key: value for key, value in table_options.items() if key in _TABLE_OPTION_NAMES
    }

    raw_start = table_options.get("sync_start_date")
    sync_start_date = (
        _parse_iso_date(raw_start, "sync_start_date") if raw_start is not None else None
    )
    raw_end = table_options.get("sync_end_date")
    sync_end_date = _parse_iso_date(raw_end, "sync_end_date") if raw_end is not None else None

    timezone_name = table_options.get("reporting_timezone", "UTC")
    try:
        reporting_timezone = ZoneInfo(timezone_name)
    except (TypeError, ZoneInfoNotFoundError) as error:
        raise ValueError(f"reporting_timezone is invalid: {timezone_name!r}") from error

    raw_advertiser_ids = table_options.get("advertiser_ids", "[]")
    try:
        parsed_advertiser_ids = json.loads(raw_advertiser_ids)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("advertiser_ids must be a JSON list of non-empty strings") from error
    if not isinstance(parsed_advertiser_ids, list) or any(
        not isinstance(item, str) or not item for item in parsed_advertiser_ids
    ):
        raise ValueError("advertiser_ids must be a JSON list of non-empty strings")

    return ReadOptions(
        sync_start_date=sync_start_date,
        sync_end_date=sync_end_date,
        initial_lookback_months=_parse_positive_int(table_options, "initial_lookback_months", 12),
        lookback_window_days=_parse_positive_int(table_options, "lookback_window_days", 30),
        reporting_timezone=reporting_timezone,
        advertiser_ids=tuple(parsed_advertiser_ids),
        delivery_country_codes=_parse_delivery_country_codes(
            table_options.get("delivery_country_codes")
        ),
        max_records_per_batch=_parse_positive_int(table_options, "max_records_per_batch", 100_000),
        emit_request_telemetry=_parse_strict_bool(table_options, "emit_request_telemetry", False),
    )


def _subtract_calendar_months(value: date, months: int) -> date:
    """Subtract calendar months, clamping the day to the target month's end."""

    if months < 0:
        raise ValueError("months must be non-negative")
    zero_based_month = value.year * 12 + value.month - 1 - months
    target_year, target_month_index = divmod(zero_based_month, 12)
    target_month = target_month_index + 1
    target_day = min(value.day, calendar.monthrange(target_year, target_month)[1])
    return date(target_year, target_month, target_day)


def _initial_start(options: ReadOptions, upper_bound: date) -> date:
    if options.sync_start_date is not None:
        return options.sync_start_date
    return _subtract_calendar_months(upper_bound, options.initial_lookback_months)


def _resolve_read_state(
    start_offset: dict | None, options: ReadOptions, upper_bound: date
) -> ReadState:
    """Resolve a first load, completed checkpoint, or in-progress checkpoint."""

    effective_upper_bound = options.sync_end_date or upper_bound
    if effective_upper_bound > upper_bound:
        raise ValueError("sync_end_date cannot be after the latest complete reporting date")
    initial_start = _initial_start(options, effective_upper_bound)
    if options.sync_end_date is not None and effective_upper_bound < initial_start:
        raise ValueError("sync_end_date cannot be before the effective sync_start_date")
    if not start_offset:
        return ReadState(None, initial_start, effective_upper_bound, initial_start)

    keys = set(start_offset)
    if keys == {"high_watermark"}:
        high_watermark = _parse_iso_date(start_offset["high_watermark"], "offset high_watermark")
        replay_start = high_watermark - timedelta(days=options.lookback_window_days - 1)
        return ReadState(
            high_watermark=high_watermark,
            next_date=max(initial_start, replay_start),
            upper_bound=effective_upper_bound,
            initial_start=initial_start,
        )

    expected_keys = {"high_watermark", "next_date", "upper_bound", "initial_start"}
    if keys != expected_keys:
        raise ValueError(
            "offset must be empty, completed with high_watermark, or contain all "
            "in-progress state fields"
        )

    raw_high_watermark = start_offset["high_watermark"]
    high_watermark = (
        None
        if raw_high_watermark is None
        else _parse_iso_date(raw_high_watermark, "offset high_watermark")
    )
    stored_upper_bound = _parse_iso_date(start_offset["upper_bound"], "offset upper_bound")
    if options.sync_end_date is not None and effective_upper_bound < stored_upper_bound:
        raise ValueError("sync_end_date cannot regress an in-progress upper_bound")
    return ReadState(
        high_watermark=high_watermark,
        next_date=_parse_iso_date(start_offset["next_date"], "offset next_date"),
        upper_bound=(
            effective_upper_bound if options.sync_end_date is not None else stored_upper_bound
        ),
        initial_start=_parse_iso_date(start_offset["initial_start"], "offset initial_start"),
    )


def _next_offset(state: ReadState, telemetry: RequestTelemetry | None = None) -> dict[str, object]:
    """Serialize in-progress read state without changing the high-water mark."""

    offset = {
        "high_watermark": (
            state.high_watermark.isoformat() if state.high_watermark is not None else None
        ),
        "next_date": state.next_date.isoformat(),
        "upper_bound": state.upper_bound.isoformat(),
        "initial_start": state.initial_start.isoformat(),
    }
    if telemetry is not None:
        offset["request_telemetry"] = telemetry.as_dict()
    return offset


def _completed_offset(
    state: ReadState, telemetry: RequestTelemetry | None = None
) -> dict[str, object]:
    high_watermark = max(state.high_watermark or state.upper_bound, state.upper_bound)
    offset = {"high_watermark": high_watermark.isoformat()}
    if telemetry is not None:
        offset["request_telemetry"] = telemetry.as_dict()
    return offset


def _read_telemetry(
    start_offset: dict | None, enabled: bool
) -> tuple[dict | None, RequestTelemetry | None]:
    if not start_offset or "request_telemetry" not in start_offset:
        return start_offset, RequestTelemetry() if enabled else None
    telemetry = RequestTelemetry.from_dict(start_offset["request_telemetry"])
    state_offset = {key: value for key, value in start_offset.items() if key != "request_telemetry"}
    if not enabled:
        raise ValueError("emit_request_telemetry cannot be disabled for an instrumented offset")
    if "next_date" not in state_offset:
        telemetry = RequestTelemetry()
    return state_offset, telemetry


class GoogleCm360LakeflowConnect(LakeflowConnect):
    """Expose fixed CM360 daily performance and reach contracts."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        access_token = options.get("access_token")
        profile_id = options.get("profile_id")
        if not isinstance(access_token, str) or not access_token:
            raise ValueError("access_token is required")
        if not isinstance(profile_id, str) or not profile_id.isdigit():
            raise ValueError("profile_id must contain digits only")
        self._access_token = access_token
        self._profile_id = profile_id
        self._upper_bounds: dict[str, date] = {}
        self._terminal_offsets: dict[str, dict] = {}
        self._preflight_complete_tables: set[str] = set()
        self._profile_metadata_by_table: dict[str, ProfileMetadata] = {}
        self._delivery_country_codes: dict[str, str | None] = {}
        self._delivery_country_ids: dict[str, str | None] = {}
        self._client_instance: Cm360Client | None = None

    @property
    def _client(self) -> Cm360Client:
        if self._client_instance is None:
            self._client_instance = Cm360Client(self._access_token, self._profile_id)
        return self._client_instance

    @staticmethod
    def _resolve_delivery_country_id(response: dict, country_code: str) -> str:
        def fail(reason: str) -> None:
            raise Cm360ConfigurationError(
                f"CM360 country dimension lookup did not return one exact result (reason={reason})"
            )

        country_name = DELIVERY_COUNTRY_NAMES[country_code]
        items = response.get("items")
        if response.get("nextPageToken") not in (None, ""):
            fail("paginated")
        if not isinstance(items, list):
            fail("items_missing")
        if len(items) != 1:
            fail(f"item_count_{len(items)}")
        if not isinstance(items[0], dict):
            fail("item_not_object")
        item = items[0]
        if item.get("dimensionName") != "country":
            fail("dimension_name_mismatch")
        if item.get("value") != country_name:
            fail("value_mismatch")
        if item.get("matchType", "EXACT") != "EXACT":
            fail("match_type_mismatch")
        return _required_numeric_id(item, "id", "country dimension lookup")

    def _validate_preflight(
        self,
        table_name: str,
        report: dict,
        telemetry: RequestTelemetry | None = None,
    ) -> None:
        if table_name in self._preflight_complete_tables:
            return
        profile = self._client.get_profile(**_telemetry_kwargs(telemetry))
        metadata = _build_profile_metadata(profile)
        self._client.validate_compatible_fields(report, **_telemetry_kwargs(telemetry))
        self._profile_metadata_by_table[table_name] = metadata
        self._delivery_country_codes[table_name] = None
        self._delivery_country_ids[table_name] = None
        self._preflight_complete_tables.add(table_name)

    def _prepare_report(
        self,
        table_name: str,
        report_date: date,
        options: ReadOptions,
        upper_bound: date,
        telemetry: RequestTelemetry | None = None,
    ) -> dict:
        report_builder = TABLE_CONTRACTS[table_name].report_builder
        country_code = options.delivery_country_codes[0] if options.delivery_country_codes else None
        if country_code is None:
            report = report_builder(report_date, options.advertiser_ids)
            if (
                table_name in self._preflight_complete_tables
                and self._delivery_country_codes[table_name] is not None
            ):
                raise Cm360ConfigurationError(
                    "delivery_country_codes cannot change within one connector update"
                )
            self._validate_preflight(table_name, report, telemetry)
            return report

        if table_name in self._preflight_complete_tables:
            if country_code != self._delivery_country_codes[table_name]:
                raise Cm360ConfigurationError(
                    "delivery_country_codes cannot change within one connector update"
                )
            return report_builder(
                report_date,
                options.advertiser_ids,
                self._delivery_country_ids[table_name],
            )

        profile = self._client.get_profile(**_telemetry_kwargs(telemetry))
        metadata = _build_profile_metadata(profile)
        country_name = DELIVERY_COUNTRY_NAMES[country_code]
        response = self._client.query_dimension_values(
            "country",
            country_name,
            _initial_start(options, upper_bound),
            upper_bound,
            **_telemetry_kwargs(telemetry),
        )
        delivery_country_id = self._resolve_delivery_country_id(response, country_code)

        report = report_builder(
            report_date,
            options.advertiser_ids,
            delivery_country_id,
        )
        self._client.validate_compatible_fields(report, **_telemetry_kwargs(telemetry))
        self._profile_metadata_by_table[table_name] = metadata
        self._delivery_country_codes[table_name] = country_code
        self._delivery_country_ids[table_name] = delivery_country_id
        self._preflight_complete_tables.add(table_name)
        return report

    def _execute_daily_report(
        self,
        table_name: str,
        report_date: date,
        options: ReadOptions,
        telemetry: RequestTelemetry | None,
        upper_bound: date,
    ) -> list[dict]:
        report = self._prepare_report(
            table_name,
            report_date,
            options,
            upper_bound,
            telemetry,
        )
        profile_metadata = self._profile_metadata_by_table.get(table_name)
        if profile_metadata is None:
            raise Cm360ConfigurationError("CM360 profile metadata was not initialized")

        report_id = self._client.insert_report(report, **_telemetry_kwargs(telemetry))
        cleanup_succeeded = False
        try:
            file_id = self._client.run_report(report_id, **_telemetry_kwargs(telemetry))
            file_resource = _poll_until_available(self._client, report_id, file_id, telemetry)
            csv_text = self._client.download_file(
                report_id, file_id, **_telemetry_kwargs(telemetry)
            )
            rows = _parse_report_csv(
                csv_text,
                profile_id=self._profile_id,
                expected_report_date=report_date,
                profile_metadata=profile_metadata,
                file_last_modified_at=file_resource.get("lastModifiedTime", ""),
                expected_delivery_country=(
                    DELIVERY_COUNTRY_NAMES[options.delivery_country_codes[0]]
                    if options.delivery_country_codes
                    else None
                ),
                field_specs=TABLE_CONTRACTS[table_name].field_specs,
                key_fields=tuple(
                    key
                    for key in TABLE_CONTRACTS[table_name].metadata["primary_keys"]
                    if key not in ("profile_id", "report_date")
                ),
            )
        finally:
            try:
                self._client.delete_report(report_id, **_telemetry_kwargs(telemetry))
                cleanup_succeeded = True
            except Exception:  # pylint: disable=broad-exception-caught
                logger.warning(
                    "CM360 temporary report cleanup failed for profile_id=%s report_date=%s",
                    self._profile_id,
                    report_date,
                )
        if telemetry is not None and cleanup_succeeded:
            telemetry.increment("daily_reports_completed")
        return rows

    def _period_export(self, report: dict, options: ReadOptions, fields: tuple) -> list[dict]:
        self._client.validate_compatible_fields(report)
        report_id = self._client.insert_report(report)
        try:
            file_id = self._client.run_report(report_id)
            resource = _poll_until_available(self._client, report_id, file_id, None)
            text = self._client.download_file(report_id, file_id)
            csv_rows = list(csv.reader(io.StringIO(text)))
            zone = next(
                (r[1] for r in csv_rows if len(r) == 2 and r[0] == "Report Time Zone"), None
            )
            if zone != str(options.reporting_timezone):
                raise Cm360ConfigurationError(
                    "CM360 report timezone differs from reporting_timezone"
                )
            native_range = next(
                (r[1] for r in csv_rows if len(r) == 2 and r[0] == "Date Range"), None
            )
            bounds = (report.get("reachCriteria") or report.get("criteria"))["dateRange"]
            expected_range = " - ".join(
                date.fromisoformat(bounds[key]).strftime("%d/%m/%Y")
                for key in ("startDate", "endDate")
            )
            if native_range != expected_range:
                raise Cm360DataError("CM360 CSV period differs from the requested period")
            if not any(row and row[0] == "Grand Total:" and "---" in row[1:] for row in csv_rows):
                raise Cm360DataError("CM360 period export is incomplete: missing native footer")
            return _parse_report_csv(
                text,
                profile_id=self._profile_id,
                expected_report_date=options.sync_end_date,
                profile_metadata=self._period_profile_metadata,
                file_last_modified_at=resource.get("lastModifiedTime", ""),
                expected_delivery_country=DELIVERY_COUNTRY_NAMES[options.delivery_country_codes[0]],
                field_specs=fields,
            )
        finally:
            self._client.delete_report(report_id)

    def _period_campaigns(self, options: ReadOptions) -> list[dict]:
        self._period_profile_metadata = _build_profile_metadata(self._client.get_profile())
        response = self._client.query_dimension_values(
            "country",
            DELIVERY_COUNTRY_NAMES[options.delivery_country_codes[0]],
            options.sync_start_date,
            options.sync_end_date,
        )
        self._period_country_id = self._resolve_delivery_country_id(
            response, options.delivery_country_codes[0]
        )
        report = {
            "name": "lakeflow-google-cm360-period-metadata",
            "type": "STANDARD",
            "format": "CSV",
            "criteria": {
                "dateRange": {
                    "startDate": options.sync_start_date.isoformat(),
                    "endDate": options.sync_end_date.isoformat(),
                },
                "dimensions": [
                    {"name": f.api_name}
                    for f in CAMPAIGN_PERIOD_FIELD_SPECS
                    if f.api_name != "impressions"
                ],
                "metricNames": ["impressions"],
                "dimensionFilters": [
                    {"dimensionName": "country", "id": self._period_country_id},
                    *[{"dimensionName": "advertiser", "id": i} for i in options.advertiser_ids],
                ],
            },
        }
        return self._period_export(report, options, CAMPAIGN_PERIOD_FIELD_SPECS)

    def _period_campaign_row(self, campaign: dict, options: ReadOptions) -> dict:
        lower, upper, coverage = _campaign_period(
            campaign["campaign_start_date"],
            campaign["campaign_end_date"],
            options.sync_end_date,
        )
        row = {k: v for k, v in campaign.items() if k not in {"report_date", "impressions"}}
        row.update(
            {
                "period_start": lower.isoformat() if lower else None,
                "period_end": upper.isoformat() if upper else None,
                "source_cutoff_date": options.sync_end_date.isoformat(),
                "metadata_anchor": options.sync_start_date.isoformat(),
                "reporting_timezone": str(options.reporting_timezone),
                "coverage_status": coverage,
                "unique_reach_impression_reach": None,
            }
        )
        if coverage == "COMPLETE":
            report = _build_period_reach_report(
                lower, upper, campaign["campaign_id"], self._period_country_id
            )
            values = self._period_export(report, options, PERIOD_REACH_FIELD_SPECS)
            if len(values) > 1 or any(
                v["campaign_id"] != campaign["campaign_id"]
                or v["advertiser_id"] != campaign["advertiser_id"]
                for v in values
            ):
                raise Cm360DataError("CM360 period report returned an unexpected campaign")
            if values:
                row["unique_reach_impression_reach"] = values[0]["unique_reach_impression_reach"]
                row["report_file_last_modified_at"] = values[0]["report_file_last_modified_at"]
        return row

    @staticmethod
    def _validate_table(table_name: str) -> None:
        if table_name not in TABLE_CONTRACTS:
            raise ValueError(
                f"Table {table_name!r} is not supported. "
                f"Supported tables: {list(TABLE_CONTRACTS)!r}"
            )

    def list_tables(self) -> list[str]:
        return list(TABLE_CONTRACTS)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        del table_options
        self._validate_table(table_name)
        return TABLE_CONTRACTS[table_name].schema

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        del table_options
        self._validate_table(table_name)
        metadata = TABLE_CONTRACTS[table_name].metadata
        return {
            **metadata,
            "primary_keys": list(metadata["primary_keys"]),
        }

    def read_table(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        options = _parse_read_options(table_options)
        if table_name == PERIOD_REACH_TABLE_NAME:
            if (
                options.sync_start_date is None
                or options.sync_end_date is None
                or not options.delivery_country_codes
            ):
                raise Cm360ConfigurationError(
                    "Period snapshot requires explicit metadata bounds and one delivery country"
                )
            if options.sync_start_date > options.sync_end_date:
                raise ValueError("Period metadata bounds require start before or equal to end")
            if options.sync_end_date >= datetime.now(options.reporting_timezone).date():
                raise ValueError("Period snapshot requires a completed reporting date")
            # The framework reads once and discards offsets. Materialize the whole
            # snapshot before returning; a late failure cannot publish a prefix.
            campaigns = self._period_campaigns(options)
            rows = [self._period_campaign_row(campaign, options) for campaign in campaigns]
            return iter(rows), {}
        if (
            options.delivery_country_codes
            and not TABLE_CONTRACTS[table_name].supports_delivery_country
        ):
            # Connector options are shared by all objects of a pipeline. A table
            # without a country dimension ignores the pipeline's country filter.
            options = dataclass_replace(options, delivery_country_codes=())
        if (
            table_name in self._terminal_offsets
            and start_offset == self._terminal_offsets[table_name]
        ):
            return iter([]), start_offset
        state_offset, telemetry = _read_telemetry(start_offset, options.emit_request_telemetry)
        upper_bound = self._upper_bounds.setdefault(
            table_name,
            datetime.now(options.reporting_timezone).date() - timedelta(days=1),
        )

        state = _resolve_read_state(state_offset, options, upper_bound)
        if state.next_date > state.upper_bound:
            completed_offset = _completed_offset(state, telemetry)
            self._terminal_offsets[table_name] = completed_offset
            return iter([]), completed_offset

        rows = self._execute_daily_report(
            table_name, state.next_date, options, telemetry, state.upper_bound
        )
        if len(rows) > options.max_records_per_batch:
            logger.warning(
                "CM360 daily report exceeds max_records_per_batch profile_id=%s "
                "report_date=%s count=%s target=%s",
                self._profile_id,
                state.next_date,
                len(rows),
                options.max_records_per_batch,
            )

        if state.next_date == state.upper_bound:
            completed_offset = _completed_offset(state, telemetry)
            self._terminal_offsets[table_name] = completed_offset
            return iter(rows), completed_offset

        next_state = ReadState(
            high_watermark=state.high_watermark,
            next_date=state.next_date + timedelta(days=1),
            upper_bound=state.upper_bound,
            initial_start=state.initial_start,
        )
        return iter(rows), _next_offset(next_state, telemetry)
