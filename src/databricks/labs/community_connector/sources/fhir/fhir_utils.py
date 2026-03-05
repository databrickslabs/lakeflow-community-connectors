"""HTTP client, authentication, bundle pagination, and record extraction for FHIR R4.

Key design decisions (from fhir_api_doc.md):
- SmartAuthClient handles jwt_assertion, client_secret, and none auth types.
- Token is cached and refreshed 30s before expiry (tokens are short-lived: ~300s).
- iter_bundle_pages follows Bundle.link[relation=next] per FHIR R4 spec.
- extract_record returns raw field values (no type conversion) -- framework handles coercion.
- Absent struct fields return None, not {}.
- All HTTP calls have timeout=60 to prevent hangs on slow FHIR servers.
"""

import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional
from urllib.parse import urljoin

import requests

from databricks.labs.community_connector.sources.fhir.fhir_constants import (
    HTTP_TIMEOUT, INITIAL_BACKOFF, MAX_RETRIES, RETRIABLE_STATUS_CODES, TOKEN_TIMEOUT,
)


class SmartAuthClient:
    """Fetches and caches a SMART on FHIR Bearer token.

    auth_type values:
    - "jwt_assertion": SMART Backend Services with RSA/EC private key (RFC 7523)
    - "client_secret": OAuth2 client credentials with symmetric secret
    - "none": No authentication (open servers, dev/test only)
    """

    def __init__(self, token_url: str, client_id: str, auth_type: str,
                 private_key_pem: str = "", client_secret: str = "", scope: str = "") -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._auth_type = auth_type
        self._private_key_pem = private_key_pem
        self._client_secret = client_secret
        self._scope = scope
        self._access_token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

    def get_token(self) -> str:
        if self._auth_type == "none":
            return ""
        if self._access_token and self._expires_at:
            if datetime.now(timezone.utc) < self._expires_at:
                return self._access_token
        self._refresh_token()
        return self._access_token

    def _refresh_token(self) -> None:
        if self._auth_type == "jwt_assertion":
            data = self._jwt_assertion_data()
        elif self._auth_type == "client_secret":
            data = self._client_secret_data()
        else:
            raise ValueError(f"Unsupported auth_type: {self._auth_type!r}. "
                             f"Use 'jwt_assertion', 'client_secret', or 'none'.")

        resp = requests.post(
            self._token_url, data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=TOKEN_TIMEOUT,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"SMART token request failed (HTTP {resp.status_code}): {resp.text}")
        body = resp.json()
        self._access_token = body["access_token"]
        expires_in = int(body.get("expires_in", 300))
        self._expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 30)

    def _jwt_assertion_data(self) -> dict:
        import jwt  # PyJWT

        now = datetime.now(timezone.utc)
        payload = {
            "iss": self._client_id, "sub": self._client_id, "aud": self._token_url,
            "jti": str(uuid.uuid4()), "iat": now, "exp": now + timedelta(minutes=5),
        }
        assertion = jwt.encode(payload, self._private_key_pem, algorithm="RS256")
        data = {
            "grant_type": "client_credentials",
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": assertion,
        }
        if self._scope:
            data["scope"] = self._scope
        return data

    def _client_secret_data(self) -> dict:
        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        if self._scope:
            data["scope"] = self._scope
        return data


class FhirHttpClient:
    """HTTP client for FHIR R4 REST API with auth injection and retry logic.

    Retries on 429/500/503 with exponential backoff. Honors Retry-After on 429.
    All requests have timeout=60 to prevent hangs.
    """

    def __init__(self, base_url: str, auth_client: SmartAuthClient) -> None:
        self._base_url = base_url.rstrip("/") + "/"
        self._auth_client = auth_client
        self._session = requests.Session()

    def _headers(self) -> dict:
        token = self._auth_client.get_token()
        h = {"Accept": "application/fhir+json"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def get(self, resource_type: str, params: Optional[dict] = None) -> requests.Response:
        url = urljoin(self._base_url, resource_type)
        return self._get_url(url, params=params)

    def _get_url(self, url: str, params: Optional[dict] = None) -> requests.Response:
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            resp = self._session.get(url, params=params, headers=self._headers(), timeout=HTTP_TIMEOUT)
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp
            if attempt < MAX_RETRIES - 1:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_seconds = float(retry_after)
                    except ValueError:
                        sleep_seconds = backoff  # Retry-After is an HTTP-date; fall back to backoff
                else:
                    sleep_seconds = backoff
                time.sleep(sleep_seconds)
                backoff *= 2
        return resp


def _next_link(bundle: dict) -> Optional[str]:
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None


def iter_bundle_pages(
    client: FhirHttpClient,
    resource_type: str,
    params: Optional[dict] = None,
    max_records: Optional[int] = None,
) -> Iterator[dict]:
    """Yield FHIR resources from a paginated search, following Bundle next links."""
    count = 0
    resp = client.get(resource_type, params=params)
    if resp.status_code != 200:
        raise RuntimeError(
            f"FHIR search failed for {resource_type} (HTTP {resp.status_code}): {resp.text}"
        )

    while True:
        bundle = resp.json()
        if bundle.get("resourceType") != "Bundle":
            raise RuntimeError(
                f"Expected Bundle for {resource_type}, got: {bundle.get('resourceType')}"
            )
        for entry in bundle.get("entry", []):
            resource = entry.get("resource")
            if resource:
                yield resource
                count += 1
                if max_records is not None and count >= max_records:
                    return

        next_url = _next_link(bundle)
        if not next_url:
            return

        resp = client._get_url(next_url)
        if resp.status_code != 200:
            raise RuntimeError(
                f"FHIR pagination failed for {resource_type} (HTTP {resp.status_code}): {resp.text}"
            )


def _safe(d, *keys, default=None):
    """Safely navigate nested dicts. Returns default if any key is missing."""
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
        if d is None:
            return default
    return d


def extract_record(resource: dict, resource_type: str) -> dict:
    """Map a raw FHIR resource to hybrid schema format (typed fields + raw_json).

    Returns raw field values -- do NOT pre-convert types; the framework handles coercion.
    Absent struct fields return None, not {}.
    """
    meta = resource.get("meta") or {}
    record = {
        "id": resource.get("id"),
        "resourceType": resource.get("resourceType", resource_type),
        "lastUpdated": meta.get("lastUpdated"),
        "raw_json": json.dumps(resource),
    }
    extractor = _EXTRACTORS.get(resource_type)
    if extractor:
        record.update(extractor(resource))
    return record


# ---------------------------------------------------------------------------
# Resource-specific field extractors
# ---------------------------------------------------------------------------

def _patient(r):
    name = ((r.get("name") or [{}])[0]) or {}
    return {
        "gender": r.get("gender"),
        "birthDate": r.get("birthDate"),
        "active": r.get("active"),
        "name_text": name.get("text"),
        "name_family": name.get("family"),
    }


def _observation(r):
    code = r.get("code") or {}
    codings = code.get("coding") or [{}]
    coding = (codings[0] if codings else None) or {}
    vq = r.get("valueQuantity") or {}
    return {
        "status": r.get("status"),
        "code_text": code.get("text"),
        "code_system": coding.get("system"),
        "code_code": coding.get("code"),
        "subject_reference": _safe(r, "subject", "reference"),
        "effective_datetime": r.get("effectiveDateTime"),
        "value_quantity_value": vq.get("value"),
        "value_quantity_unit": vq.get("unit"),
        "value_string": r.get("valueString"),
        "issued": r.get("issued"),
    }


def _condition(r):
    cs = _safe(r, "clinicalStatus", "coding") or [{}]
    vs = _safe(r, "verificationStatus", "coding") or [{}]
    return {
        "clinical_status": (cs[0] or {}).get("code"),
        "verification_status": (vs[0] or {}).get("code"),
        "code_text": _safe(r, "code", "text"),
        "subject_reference": _safe(r, "subject", "reference"),
        "onset_datetime": r.get("onsetDateTime"),
        "recorded_date": r.get("recordedDate"),
    }


def _encounter(r):
    class_ = r.get("class") or {}
    period = r.get("period") or {}
    return {
        "status": r.get("status"),
        "class_code": class_.get("code"),
        "subject_reference": _safe(r, "subject", "reference"),
        "period_start": period.get("start"),
        "period_end": period.get("end"),
    }


def _procedure(r):
    return {
        "status": r.get("status"),
        "code_text": _safe(r, "code", "text"),
        "subject_reference": _safe(r, "subject", "reference"),
        "performed_datetime": r.get("performedDateTime"),
    }


def _medication_request(r):
    return {
        "status": r.get("status"),
        "intent": r.get("intent"),
        "medication_text": _safe(r, "medicationCodeableConcept", "text"),
        "subject_reference": _safe(r, "subject", "reference"),
        "authored_on": r.get("authoredOn"),
    }


def _diagnostic_report(r):
    return {
        "status": r.get("status"),
        "code_text": _safe(r, "code", "text"),
        "subject_reference": _safe(r, "subject", "reference"),
        "effective_datetime": r.get("effectiveDateTime"),
        "issued": r.get("issued"),
    }


def _allergy(r):
    cs = _safe(r, "clinicalStatus", "coding") or [{}]
    vs = _safe(r, "verificationStatus", "coding") or [{}]
    return {
        "clinical_status": (cs[0] or {}).get("code"),
        "verification_status": (vs[0] or {}).get("code"),
        "code_text": _safe(r, "code", "text"),
        "patient_reference": _safe(r, "patient", "reference"),
        "recorded_date": r.get("recordedDate"),
    }


def _immunization(r):
    return {
        "status": r.get("status"),
        "vaccine_code_text": _safe(r, "vaccineCode", "text"),
        "patient_reference": _safe(r, "patient", "reference"),
        "occurrence_datetime": r.get("occurrenceDateTime"),
    }


def _coverage(r):
    payors = r.get("payor") or [None]
    period = r.get("period") or {}
    return {
        "status": r.get("status"),
        "beneficiary_reference": _safe(r, "beneficiary", "reference"),
        "payor_reference": _safe(payors[0] or {}, "reference"),
        "period_start": period.get("start"),
        "period_end": period.get("end"),
    }


def _care_plan(r):
    period = r.get("period") or {}
    return {
        "status": r.get("status"),
        "intent": r.get("intent"),
        "subject_reference": _safe(r, "subject", "reference"),
        "period_start": period.get("start"),
        "period_end": period.get("end"),
    }


def _goal(r):
    return {
        "lifecycle_status": r.get("lifecycleStatus"),
        "description_text": _safe(r, "description", "text"),
        "subject_reference": _safe(r, "subject", "reference"),
        "start_date": r.get("startDate"),
    }


def _device(r):
    names = r.get("deviceName") or [{}]
    return {
        "status": r.get("status"),
        "device_name": (names[0] or {}).get("name"),
        "type_text": _safe(r, "type", "text"),
        "patient_reference": _safe(r, "patient", "reference"),
    }


def _document_reference(r):
    return {
        "status": r.get("status"),
        "type_text": _safe(r, "type", "text"),
        "subject_reference": _safe(r, "subject", "reference"),
        "date": r.get("date"),
    }


_EXTRACTORS = {
    "Patient": _patient,
    "Observation": _observation,
    "Condition": _condition,
    "Encounter": _encounter,
    "Procedure": _procedure,
    "MedicationRequest": _medication_request,
    "DiagnosticReport": _diagnostic_report,
    "AllergyIntolerance": _allergy,
    "Immunization": _immunization,
    "Coverage": _coverage,
    "CarePlan": _care_plan,
    "Goal": _goal,
    "Device": _device,
    "DocumentReference": _document_reference,
}
