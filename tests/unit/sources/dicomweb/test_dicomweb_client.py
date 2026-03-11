"""
Unit tests for DICOMwebClient — all HTTP calls are mocked via unittest.mock.
"""

import json
import pathlib
import urllib.error
import urllib.parse
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from databricks.labs.community_connector.sources.dicomweb.dicomweb_client import (
    DICOMwebClient,
    _extract_first_multipart_part,
    _parse_boundary,
)

BASE_URL = "https://dicomweb.example.com"
FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


def _load(name: str) -> list[dict]:
    return json.loads((FIXTURES_DIR / f"{name}.json").read_text())


class MockResponse:
    """Minimal mock of http.client.HTTPResponse."""

    def __init__(
        self, body: bytes, status: int = 200, content_type: str = "application/dicom+json"
    ):
        self.status = status
        self._body = body
        self.headers = {"Content-Type": content_type}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def _mock_http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url="https://dicomweb.example.com/studies",
        code=code,
        msg=f"HTTP {code}",
        hdrs=MagicMock(get=lambda k, d="": d),
        fp=BytesIO(b""),
    )


# ---------------------------------------------------------------------------
# Construction & auth
# ---------------------------------------------------------------------------


class TestClientConstruction:
    def test_no_auth(self):
        client = DICOMwebClient(BASE_URL, auth={})
        assert client.base_url == BASE_URL
        assert "Authorization" not in client._default_headers

    def test_no_auth_none(self):
        client = DICOMwebClient(BASE_URL, auth=None)
        assert "Authorization" not in client._default_headers

    def test_basic_auth(self):
        client = DICOMwebClient(BASE_URL, auth={"username": "u", "password": "p"})
        assert "Authorization" in client._default_headers
        assert client._default_headers["Authorization"].startswith("Basic ")

    def test_bearer_auth(self):
        client = DICOMwebClient(BASE_URL, auth={"token": "tok123"})
        assert "Authorization" in client._default_headers
        assert client._default_headers["Authorization"] == "Bearer tok123"

    def test_trailing_slash_stripped(self):
        client = DICOMwebClient(BASE_URL + "/", auth={})
        assert not client.base_url.endswith("/")


# ---------------------------------------------------------------------------
# QIDO-RS queries
# ---------------------------------------------------------------------------


class TestQIDORS:
    def test_query_studies_returns_records(self, studies_response):
        body = json.dumps(studies_response).encode()
        with patch("urllib.request.urlopen", return_value=MockResponse(body)):
            client = DICOMwebClient(BASE_URL)
            result = client.query_studies("20231215-20231216", limit=100, offset=0)
        assert len(result) == 2
        assert (
            result[0]["0020000D"]["Value"][0] == "1.2.840.113619.2.5.1762583153.215519.978957063.78"
        )

    def test_query_studies_passes_params(self):
        captured = {}

        def fake_urlopen(req, timeout=None):
            captured["url"] = req.full_url
            return MockResponse(b"[]")

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            client = DICOMwebClient(BASE_URL)
            client.query_studies("20231201-20231215", limit=50, offset=100)

        parsed = urllib.parse.urlparse(captured["url"])
        qs = urllib.parse.parse_qs(parsed.query)
        assert qs["StudyDate"] == ["20231201-20231215"]
        assert qs["limit"] == ["50"]
        assert qs["offset"] == ["100"]

    def test_query_studies_204_returns_empty(self):
        with patch("urllib.request.urlopen", return_value=MockResponse(b"", status=204)):
            client = DICOMwebClient(BASE_URL)
            result = client.query_studies("20231215-20231216")
        assert result == []

    def test_query_series_for_study_returns_records(self, series_response):
        body = json.dumps(series_response).encode()
        with patch("urllib.request.urlopen", return_value=MockResponse(body)):
            client = DICOMwebClient(BASE_URL)
            result = client.query_series_for_study("1.2.3")
        assert len(result) == 3

    def test_query_series_for_study_204_returns_empty(self):
        with patch("urllib.request.urlopen", return_value=MockResponse(b"", status=204)):
            client = DICOMwebClient(BASE_URL)
            assert client.query_series_for_study("1.2.3") == []

    def test_query_instances_for_series_returns_records(self, instances_response):
        body = json.dumps(instances_response).encode()
        with patch("urllib.request.urlopen", return_value=MockResponse(body)):
            client = DICOMwebClient(BASE_URL)
            result = client.query_instances_for_series("1.2.3", "1.2.3.4")
        assert len(result) == 3

    def test_http_error_raises(self):
        with patch("urllib.request.urlopen", side_effect=_mock_http_error(500)):
            client = DICOMwebClient(BASE_URL)
            with pytest.raises(urllib.error.HTTPError):
                client.query_studies("20231215-20231216")

    def test_empty_body_returns_empty_list(self):
        with patch("urllib.request.urlopen", return_value=MockResponse(b"", status=200)):
            client = DICOMwebClient(BASE_URL)
            result = client.query_studies("20231215-20231216")
        assert result == []


# ---------------------------------------------------------------------------
# WADO-RS
# ---------------------------------------------------------------------------


FAKE_DCM = b"DICM" + b"\x00" * 128  # minimal fake DICOM preamble


class TestWADORS:
    def test_retrieve_instance_raw(self):
        with patch(
            "urllib.request.urlopen",
            return_value=MockResponse(FAKE_DCM, content_type="application/dicom"),
        ):
            client = DICOMwebClient(BASE_URL)
            result = client.retrieve_instance("1.2.3", "1.2.3.4", "1.2.3.4.5")
        assert result == FAKE_DCM

    def test_retrieve_instance_multipart(self):
        boundary = "myboundary"
        body = (
            (f"--{boundary}\r\nContent-Type: application/dicom\r\n\r\n").encode()
            + FAKE_DCM
            + f"\r\n--{boundary}--\r\n".encode()
        )
        ct = f'multipart/related; type="application/dicom"; boundary="{boundary}"'
        with patch("urllib.request.urlopen", return_value=MockResponse(body, content_type=ct)):
            client = DICOMwebClient(BASE_URL)
            result = client.retrieve_instance("1.2.3", "1.2.3.4", "1.2.3.4.5")
        assert result == FAKE_DCM

    def test_retrieve_instance_404_raises(self):
        with patch("urllib.request.urlopen", side_effect=_mock_http_error(404)):
            client = DICOMwebClient(BASE_URL)
            with pytest.raises(urllib.error.HTTPError):
                client.retrieve_instance("x", "y", "z")

    def test_retrieve_instance_frames(self):
        fake_frame = b"\xff\xd8\xff\xe0JFIF"
        with patch(
            "urllib.request.urlopen",
            return_value=MockResponse(fake_frame, content_type="image/jpeg"),
        ):
            client = DICOMwebClient(BASE_URL)
            result = client.retrieve_instance_frames("1.2.3", "1.2.3.4", "1.2.3.4.5")
        assert result == fake_frame

    def test_retrieve_instance_frames_multipart(self):
        fake_frame = b"\xff\xd8\xff\xe0JFIF"
        boundary = "frameboundary"
        body = (
            f"--{boundary}\r\nContent-Type: image/jpeg\r\n\r\n".encode()
            + fake_frame
            + f"\r\n--{boundary}--\r\n".encode()
        )
        ct = f'multipart/related; type="image/jpeg"; boundary="{boundary}"'
        with patch("urllib.request.urlopen", return_value=MockResponse(body, content_type=ct)):
            client = DICOMwebClient(BASE_URL)
            result = client.retrieve_instance_frames("1.2.3", "1.2.3.4", "1.2.3.4.5")
        assert result == fake_frame

    def test_retrieve_series_metadata(self, instances_response):
        body = json.dumps(instances_response).encode()
        with patch("urllib.request.urlopen", return_value=MockResponse(body)):
            client = DICOMwebClient(BASE_URL)
            result = client.retrieve_series_metadata("1.2.3", "1.2.3.4")
        assert len(result) == 3

    def test_retrieve_series_metadata_204_returns_empty(self):
        with patch("urllib.request.urlopen", return_value=MockResponse(b"", status=204)):
            client = DICOMwebClient(BASE_URL)
            assert client.retrieve_series_metadata("1.2.3", "1.2.3.4") == []


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
