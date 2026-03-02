"""
HTTP client for DICOMweb QIDO-RS and WADO-RS endpoints.

Supports three authentication modes:
    none    – open endpoints (test servers, Orthanc dev)
    basic   – HTTP Basic Auth (username + password)
    bearer  – Authorization: Bearer <token>
"""

from __future__ import annotations

import base64
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

# Default socket timeout (seconds) — used for connect + read
_DEFAULT_TIMEOUT = 60


class DICOMwebClient:
    """Thin HTTP wrapper around a DICOMweb endpoint (QIDO-RS + WADO-RS)."""

    def __init__(
        self,
        base_url: str,
        auth_type: str = "none",
        username: str | None = None,
        password: str | None = None,
        token: str | None = None,
        timeout: int = _DEFAULT_TIMEOUT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        # Accept both a plain int and the legacy (connect, read) tuple
        self.timeout = max(timeout) if isinstance(timeout, tuple) else timeout
        self._default_headers: dict[str, str] = {"Accept": "application/dicom+json"}

        auth_type = (auth_type or "none").lower()
        if auth_type == "basic":
            if not username or not password:
                raise ValueError("auth_type=basic requires username and password")
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            self._default_headers["Authorization"] = f"Basic {credentials}"
        elif auth_type == "bearer":
            if not token:
                raise ValueError("auth_type=bearer requires token")
            self._default_headers["Authorization"] = f"Bearer {token}"
        elif auth_type != "none":
            raise ValueError(f"Unsupported auth_type '{auth_type}'. Use: none, basic, bearer")

    # ------------------------------------------------------------------
    # Internal HTTP helper
    # ------------------------------------------------------------------

    def _get(
        self,
        url: str,
        extra_headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ):
        """Execute a GET request and return an http.client.HTTPResponse."""
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        headers = {**self._default_headers, **(extra_headers or {})}
        req = urllib.request.Request(url, headers=headers)
        return urllib.request.urlopen(req, timeout=self.timeout)

    # ------------------------------------------------------------------
    # QIDO-RS helpers
    # ------------------------------------------------------------------

    def _qido_get(self, path: str, params: dict[str, Any]) -> list[dict]:
        """Execute a QIDO-RS GET and return the parsed JSON list."""
        url = f"{self.base_url}{path}"
        logger.debug("QIDO-RS GET %s params=%s", url, params)
        resp = self._get(url, params=params)
        if resp.status == 204:
            # No content — valid empty response
            return []
        body = resp.read()
        # Some servers return empty body instead of 204
        if not body:
            return []
        return json.loads(body)

    def query_studies(
        self,
        study_date_range: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        QIDO-RS: GET /studies?StudyDate={range}&limit={n}&offset={n}

        Args:
            study_date_range: DICOMweb date range string, e.g. "20231201-20231215".
            limit:            Max records per page.
            offset:           Zero-based record offset for pagination.

        Returns:
            List of DICOM JSON objects (dicts keyed by 8-char hex tag strings).
        """
        params: dict[str, Any] = {
            "StudyDate": study_date_range,
            "limit": limit,
            "offset": offset,
        }
        return self._qido_get("/studies", params)

    def query_series_for_study(self, study_uid: str) -> list[dict]:
        """
        QIDO-RS: GET /studies/{study_uid}/series

        Hierarchical endpoint returning all series belonging to a single study.
        Required by S3 Static WADO Server and recommended by the DICOMweb standard.

        Args:
            study_uid: DICOM study UID.

        Returns:
            List of DICOM JSON objects (dicts keyed by 8-char hex tag strings).
        """
        return self._qido_get(f"/studies/{study_uid}/series", {})

    def query_instances_for_series(self, study_uid: str, series_uid: str) -> list[dict]:
        """
        QIDO-RS: GET /studies/{study_uid}/series/{series_uid}/instances

        Hierarchical endpoint returning all instances belonging to a single series.

        Args:
            study_uid:  DICOM study UID.
            series_uid: DICOM series UID.

        Returns:
            List of DICOM JSON objects (dicts keyed by 8-char hex tag strings).
        """
        return self._qido_get(f"/studies/{study_uid}/series/{series_uid}/instances", {})

    # ------------------------------------------------------------------
    # WADO-RS
    # ------------------------------------------------------------------

    def retrieve_instance(
        self,
        study_uid: str,
        series_uid: str,
        sop_uid: str,
    ) -> bytes:
        """
        WADO-RS: retrieve a raw DICOM file.

        GET {base_url}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}
        Accept: multipart/related; type="application/dicom"

        Returns:
            Raw bytes of the .dcm file (multipart/related — first part extracted).
        """
        url = f"{self.base_url}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}"
        logger.debug("WADO-RS GET %s", url)
        resp = self._get(url, extra_headers={"Accept": 'multipart/related; type="application/dicom"'})
        content_type = resp.headers.get("Content-Type", "")
        body = resp.read()
        if "multipart/related" in content_type:
            return _extract_first_multipart_part(body, content_type)
        # Some servers return raw DICOM directly
        return body

    def retrieve_instance_frames(
        self,
        study_uid: str,
        series_uid: str,
        sop_uid: str,
        frame_number: int = 1,
    ) -> bytes:
        """
        WADO-RS: retrieve a specific frame from a DICOM instance.

        Used by S3 Static WADO Server and other frame-based servers that do not
        support full DICOM file retrieval via the standard WADO-RS instance endpoint.

        GET {base_url}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/frames/{n}

        Args:
            study_uid:    DICOM study UID.
            series_uid:   DICOM series UID.
            sop_uid:      DICOM SOP instance UID.
            frame_number: 1-based frame index (default: 1).

        Returns:
            Raw bytes of the frame image (JPEG, PNG, or similar).
        """
        url = f"{self.base_url}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/frames/{frame_number}"
        logger.debug("WADO-RS frames GET %s", url)
        resp = self._get(url, extra_headers={"Accept": "image/jpeg, image/png, application/octet-stream"})
        content_type = resp.headers.get("Content-Type", "")
        body = resp.read()
        if "multipart/related" in content_type:
            return _extract_first_multipart_part(body, content_type)
        return body

    def retrieve_instance_metadata(
        self,
        study_uid: str,
        series_uid: str,
        sop_uid: str,
    ) -> dict:
        """
        WADO-RS: retrieve full DICOM JSON metadata for a single instance.

        GET {base_url}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/metadata
        Accept: application/dicom+json

        Returns the complete DICOM JSON object for this SOP instance (all tags).
        More targeted than retrieve_series_metadata() when only one instance
        is needed — avoids downloading metadata for all siblings in the series.

        Args:
            study_uid:  DICOM study UID.
            series_uid: DICOM series UID.
            sop_uid:    DICOM SOP instance UID.

        Returns:
            Single full DICOM JSON object (dict keyed by 8-char hex tag strings).
            Returns an empty dict if the server returns 204 or an empty body.
        """
        url = f"{self.base_url}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/metadata"
        logger.debug("WADO-RS instance metadata GET %s", url)
        resp = self._get(url, extra_headers={"Accept": "application/dicom+json"})
        if resp.status == 204:
            return {}
        body = resp.read()
        if not body:
            return {}
        data = json.loads(body)
        # Some servers return a list with one element, others return the object directly
        if isinstance(data, list):
            return data[0] if data else {}
        return data

    def retrieve_series_metadata(
        self,
        study_uid: str,
        series_uid: str,
    ) -> list[dict]:
        """
        WADO-RS: retrieve full DICOM JSON metadata for all instances in a series.

        GET {base_url}/studies/{study_uid}/series/{series_uid}/metadata
        Accept: application/dicom+json

        Returns a list of complete DICOM JSON objects — one per instance — with
        all tags (not just the subset returned by QIDO-RS).  Useful for populating
        the `metadata` variant column or for servers (e.g. S3 Static WADO) that
        serve instance data via this endpoint rather than via QIDO-RS /instances.

        Args:
            study_uid:  DICOM study UID.
            series_uid: DICOM series UID.

        Returns:
            List of full DICOM JSON objects (one per SOP instance).
        """
        url = f"{self.base_url}/studies/{study_uid}/series/{series_uid}/metadata"
        logger.debug("WADO-RS metadata GET %s", url)
        resp = self._get(url, extra_headers={"Accept": "application/dicom+json"})
        if resp.status == 204:
            return []
        body = resp.read()
        if not body:
            return []
        return json.loads(body)

    def probe_endpoint(self, path: str, accept: str | None = None) -> dict:
        """
        Make a lightweight probe request and return capability information.

        Used by the `diagnostics` table to test which DICOMweb endpoints are
        available on the target server without raising exceptions.

        Args:
            path:   URL path relative to base_url (already includes query string
                    if needed, e.g. "/studies?limit=1").
            accept: Optional Accept header override.

        Returns:
            dict with keys: status_code (int|None), content_type (str|None),
            latency_ms (int), error (str|None).
        """
        url = f"{self.base_url}{path}"
        extra_headers = {"Accept": accept} if accept else None
        t0 = time.monotonic()
        try:
            resp = self._get(url, extra_headers=extra_headers)
            latency_ms = int((time.monotonic() - t0) * 1000)
            return {
                "status_code": resp.status,
                "content_type": resp.headers.get("Content-Type", ""),
                "latency_ms": latency_ms,
                "error": None,
            }
        except urllib.error.HTTPError as exc:
            # HTTP error responses (4xx/5xx) are valid probe results, not failures
            latency_ms = int((time.monotonic() - t0) * 1000)
            return {
                "status_code": exc.code,
                "content_type": exc.headers.get("Content-Type", ""),
                "latency_ms": latency_ms,
                "error": None,
            }
        except Exception as exc:
            latency_ms = int((time.monotonic() - t0) * 1000)
            return {
                "status_code": None,
                "content_type": None,
                "latency_ms": latency_ms,
                "error": str(exc),
            }


# ---------------------------------------------------------------------------
# Multipart helpers
# ---------------------------------------------------------------------------


def _extract_first_multipart_part(body: bytes, content_type: str) -> bytes:
    """
    Extract the raw bytes of the first part from a MIME multipart body.

    The boundary is parsed from the Content-Type header, e.g.:
        multipart/related; type="application/dicom"; boundary="myboundary"
    """
    boundary = _parse_boundary(content_type)
    if not boundary:
        # Can't parse boundary — return whole body and hope for the best
        return body

    sep = f"--{boundary}".encode()
    parts = body.split(sep)
    # parts[0] is preamble (empty), parts[1] is first part, parts[-1] is "--\r\n"
    for part in parts[1:]:
        if part.strip() in (b"", b"--", b"--\r\n", b"--\n"):
            continue
        # Strip part headers (blank line separates headers from body)
        if b"\r\n\r\n" in part:
            return part.split(b"\r\n\r\n", 1)[1].rstrip(b"\r\n")
        if b"\n\n" in part:
            return part.split(b"\n\n", 1)[1].rstrip(b"\n")
        return part
    return body


def _parse_boundary(content_type: str) -> str | None:
    """Extract the multipart boundary from a Content-Type header value."""
    for segment in content_type.split(";"):
        segment = segment.strip()
        if segment.lower().startswith("boundary="):
            boundary = segment[len("boundary="):].strip().strip('"')
            return boundary
    return None
