"""
Unit tests for DICOMwebClient — all HTTP calls are mocked with `responses`.
"""

import json
import pathlib

import pytest
import responses as rsps_lib
from requests.exceptions import HTTPError

from databricks.labs.community_connector.sources.dicomweb.dicomweb_client import (
    DICOMwebClient,
    _extract_first_multipart_part,
    _parse_boundary,
)

BASE_URL = "https://dicomweb.example.com"
FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


def _load(name: str) -> list[dict]:
    return json.loads((FIXTURES_DIR / f"{name}.json").read_text())


# ---------------------------------------------------------------------------
# Construction & auth
# ---------------------------------------------------------------------------


class TestClientConstruction:
    def test_no_auth(self):
        client = DICOMwebClient(BASE_URL, auth_type="none")
        assert client.base_url == BASE_URL

    def test_basic_auth(self):
        client = DICOMwebClient(BASE_URL, auth_type="basic", username="u", password="p")
        assert client._session.auth is not None

    def test_basic_auth_missing_creds_raises(self):
        with pytest.raises(ValueError, match="username and password"):
            DICOMwebClient(BASE_URL, auth_type="basic")

    def test_bearer_auth(self):
        client = DICOMwebClient(BASE_URL, auth_type="bearer", token="tok123")
        assert "Authorization" in client._session.headers
        assert client._session.headers["Authorization"] == "Bearer tok123"

    def test_bearer_missing_token_raises(self):
        with pytest.raises(ValueError, match="token"):
            DICOMwebClient(BASE_URL, auth_type="bearer")

    def test_unknown_auth_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported auth_type"):
            DICOMwebClient(BASE_URL, auth_type="oauth2")

    def test_trailing_slash_stripped(self):
        client = DICOMwebClient(BASE_URL + "/", auth_type="none")
        assert not client.base_url.endswith("/")


# ---------------------------------------------------------------------------
# QIDO-RS queries
# ---------------------------------------------------------------------------


class TestQIDORS:
    @rsps_lib.activate
    def test_query_studies_returns_records(self, studies_response):
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/studies",
            json=studies_response,
            status=200,
        )
        client = DICOMwebClient(BASE_URL)
        result = client.query_studies("20231215-20231216", limit=100, offset=0)
        assert len(result) == 2
        assert result[0]["0020000D"]["Value"][0] == "1.2.840.113619.2.5.1762583153.215519.978957063.78"

    @rsps_lib.activate
    def test_query_studies_passes_params(self):
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/studies",
            json=[],
            status=200,
        )
        client = DICOMwebClient(BASE_URL)
        client.query_studies("20231201-20231215", limit=50, offset=100)
        req = rsps_lib.calls[0].request
        assert "StudyDate=20231201-20231215" in req.url
        assert "limit=50" in req.url
        assert "offset=100" in req.url

    @rsps_lib.activate
    def test_query_studies_204_returns_empty(self):
        rsps_lib.add(rsps_lib.GET, f"{BASE_URL}/studies", status=204)
        client = DICOMwebClient(BASE_URL)
        result = client.query_studies("20231215-20231216")
        assert result == []

    @rsps_lib.activate
    def test_query_series_for_study_returns_records(self, series_response):
        study_uid = "1.2.3"
        rsps_lib.add(rsps_lib.GET, f"{BASE_URL}/studies/{study_uid}/series", json=series_response, status=200)
        client = DICOMwebClient(BASE_URL)
        result = client.query_series_for_study(study_uid)
        assert len(result) == 3

    @rsps_lib.activate
    def test_query_series_for_study_204_returns_empty(self):
        study_uid = "1.2.3"
        rsps_lib.add(rsps_lib.GET, f"{BASE_URL}/studies/{study_uid}/series", status=204)
        client = DICOMwebClient(BASE_URL)
        assert client.query_series_for_study(study_uid) == []

    @rsps_lib.activate
    def test_query_instances_for_series_returns_records(self, instances_response):
        study_uid = "1.2.3"
        series_uid = "1.2.3.4"
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/studies/{study_uid}/series/{series_uid}/instances",
            json=instances_response,
            status=200,
        )
        client = DICOMwebClient(BASE_URL)
        result = client.query_instances_for_series(study_uid, series_uid)
        assert len(result) == 3

    @rsps_lib.activate
    def test_http_error_raises(self):
        rsps_lib.add(rsps_lib.GET, f"{BASE_URL}/studies", status=500)
        client = DICOMwebClient(BASE_URL)
        with pytest.raises(HTTPError):
            client.query_studies("20231215-20231216")

    @rsps_lib.activate
    def test_empty_body_returns_empty_list(self):
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/studies",
            body=b"",
            status=200,
            content_type="application/dicom+json",
        )
        client = DICOMwebClient(BASE_URL)
        result = client.query_studies("20231215-20231216")
        assert result == []


# ---------------------------------------------------------------------------
# WADO-RS
# ---------------------------------------------------------------------------

FAKE_DCM = b"DICM" + b"\x00" * 128  # minimal fake DICOM preamble


class TestWADORS:
    @rsps_lib.activate
    def test_retrieve_instance_raw(self):
        study_uid = "1.2.3"
        series_uid = "1.2.3.4"
        sop_uid = "1.2.3.4.5"
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}",
            body=FAKE_DCM,
            status=200,
            content_type="application/dicom",
        )
        client = DICOMwebClient(BASE_URL)
        result = client.retrieve_instance(study_uid, series_uid, sop_uid)
        assert result == FAKE_DCM

    @rsps_lib.activate
    def test_retrieve_instance_multipart(self):
        study_uid = "1.2.3"
        series_uid = "1.2.3.4"
        sop_uid = "1.2.3.4.5"
        boundary = "myboundary"
        body = (
            (f"--{boundary}\r\nContent-Type: application/dicom\r\n\r\n").encode()
            + FAKE_DCM
            + f"\r\n--{boundary}--\r\n".encode()
        )
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}",
            body=body,
            status=200,
            content_type=f'multipart/related; type="application/dicom"; boundary="{boundary}"',
        )
        client = DICOMwebClient(BASE_URL)
        result = client.retrieve_instance(study_uid, series_uid, sop_uid)
        assert result == FAKE_DCM

    @rsps_lib.activate
    def test_retrieve_instance_404_raises(self):
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/studies/x/series/y/instances/z",
            status=404,
        )
        client = DICOMwebClient(BASE_URL)
        with pytest.raises(HTTPError):
            client.retrieve_instance("x", "y", "z")

    @rsps_lib.activate
    def test_retrieve_instance_frames(self):
        study_uid = "1.2.3"
        series_uid = "1.2.3.4"
        sop_uid = "1.2.3.4.5"
        fake_frame = b"\xff\xd8\xff\xe0JFIF"  # JPEG magic bytes
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/frames/1",
            body=fake_frame,
            status=200,
            content_type="image/jpeg",
        )
        client = DICOMwebClient(BASE_URL)
        result = client.retrieve_instance_frames(study_uid, series_uid, sop_uid)
        assert result == fake_frame

    @rsps_lib.activate
    def test_retrieve_instance_frames_multipart(self):
        study_uid = "1.2.3"
        series_uid = "1.2.3.4"
        sop_uid = "1.2.3.4.5"
        fake_frame = b"\xff\xd8\xff\xe0JFIF"
        boundary = "frameboundary"
        body = (
            f"--{boundary}\r\nContent-Type: image/jpeg\r\n\r\n".encode()
            + fake_frame
            + f"\r\n--{boundary}--\r\n".encode()
        )
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}/frames/1",
            body=body,
            status=200,
            content_type=f'multipart/related; type="image/jpeg"; boundary="{boundary}"',
        )
        client = DICOMwebClient(BASE_URL)
        result = client.retrieve_instance_frames(study_uid, series_uid, sop_uid)
        assert result == fake_frame

    @rsps_lib.activate
    def test_retrieve_series_metadata(self, instances_response):
        study_uid = "1.2.3"
        series_uid = "1.2.3.4"
        rsps_lib.add(
            rsps_lib.GET,
            f"{BASE_URL}/studies/{study_uid}/series/{series_uid}/metadata",
            json=instances_response,
            status=200,
            content_type="application/dicom+json",
        )
        client = DICOMwebClient(BASE_URL)
        result = client.retrieve_series_metadata(study_uid, series_uid)
        assert len(result) == 3

    @rsps_lib.activate
    def test_retrieve_series_metadata_204_returns_empty(self):
        study_uid = "1.2.3"
        series_uid = "1.2.3.4"
        rsps_lib.add(rsps_lib.GET, f"{BASE_URL}/studies/{study_uid}/series/{series_uid}/metadata", status=204)
        client = DICOMwebClient(BASE_URL)
        assert client.retrieve_series_metadata(study_uid, series_uid) == []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_parse_boundary(self):
        ct = 'multipart/related; type="application/dicom"; boundary="testboundary"'
        assert _parse_boundary(ct) == "testboundary"

    def test_parse_boundary_unquoted(self):
        ct = "multipart/related; boundary=simple123"
        assert _parse_boundary(ct) == "simple123"

    def test_parse_boundary_missing(self):
        assert _parse_boundary("application/dicom") is None

    def test_extract_multipart_first_part(self):
        boundary = "abc"
        body = b"--abc\r\nContent-Type: application/dicom\r\n\r\nDICMDATA\r\n--abc--\r\n"
        result = _extract_first_multipart_part(body, f"multipart/related; boundary={boundary}")
        assert result == b"DICMDATA"
