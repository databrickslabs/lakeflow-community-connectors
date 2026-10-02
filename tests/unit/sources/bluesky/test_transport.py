import io

import pytest
import requests
from conftest import response

from databricks.labs.community_connector.sources.bluesky.errors import (
    ArchiveChanged,
    CursorTooOld,
    JetstreamError,
    ProtocolError,
    RefreshTimeout,
)
from databricks.labs.community_connector.sources.bluesky.options import Options
from databricks.labs.community_connector.sources.bluesky.transport import Transport


def test_transient_errors_reconnect(service):
    service.failures = [requests.ConnectionError("secret"), response(503), response(429)]
    with Transport(Options.parse({}, {})) as transport:
        assert transport.plan(0, 2)["sealedTipSeq"] == 2
    assert len(service.calls) == 4


def test_retry_after_exceeds_budget(service):
    service.failures = [response(429, headers={"Retry-After": "86400"})]
    with Transport(Options.parse({}, {})) as transport:
        with pytest.raises(RefreshTimeout):
            transport.plan(0)
    assert len(service.calls) == 1


@pytest.mark.parametrize("status", [401, 403, 301, 400, 418])
def test_permanent_errors_redacted_and_not_retried(service, status):
    service.failures = [response(status, b'{"error":"supersecret"}')]
    with Transport(Options.parse({"api_key": "supersecret"}, {})) as transport:
        with pytest.raises(JetstreamError) as caught:
            transport.plan(0)
    assert "supersecret" not in str(caught.value)
    assert len(service.calls) == 1


def test_expired_cursor(service):
    service.failures = [response(400, b'{"error":"CursorTooOld"}')]
    with Transport(Options.parse({}, {})) as transport:
        with pytest.raises(CursorTooOld):
            transport.plan(0)


@pytest.mark.parametrize("status", [404, 410, 412])
def test_archive_changed_reports_status_without_secret(service, status):
    service.failures = [response(status, b'{"error":"supersecret"}')]
    with Transport(Options.parse({"api_key": "supersecret"}, {})) as transport:
        with pytest.raises(ArchiveChanged, match=f"HTTP {status}") as caught:
            transport.range("seg_0000000000.jss", service.checksum, 0, 255)
    assert "supersecret" not in str(caught.value)


def test_retry_exhaustion(service):
    service.failures = [requests.Timeout("secret")] * 4
    with Transport(Options.parse({}, {})) as transport:
        with pytest.raises(JetstreamError, match="exhausted") as caught:
            transport.plan(0)
        assert "secret" not in str(caught.value)


def test_deadline(service):
    with Transport(Options.parse({}, {})) as transport:
        transport.deadline = 0
        with pytest.raises(RefreshTimeout):
            transport.plan(0)
    assert not service.calls


def test_range_resume_after_clean_quota_truncation(service):
    tag = f'"{service.checksum}"'
    service.failures = [
        response(
            206,
            service.data[:10],
            {"ETag": tag, "Content-Range": f"bytes 0-255/{len(service.data)}"},
        )
    ]
    with Transport(Options.parse({}, {})) as transport:
        header, _ = transport.range("seg_0000000000.jss", service.checksum, 0, 255)
    assert header == service.data[:256]
    assert service.calls[1].headers["Range"] == "bytes=10-255"


def test_range_resume_after_socket_failure(service):
    class BrokenBody(io.BytesIO):
        def read(self, size=-1):
            if self.tell() == 10:
                raise requests.ConnectionError("interrupted")
            return super().read(min(size, 10))

    first = response(
        206,
        headers={
            "ETag": f'"{service.checksum}"',
            "Content-Range": f"bytes 0-255/{len(service.data)}",
        },
    )
    first.raw = BrokenBody(service.data)
    service.failures = [first]
    with Transport(Options.parse({}, {})) as transport:
        header, _ = transport.range("seg_0000000000.jss", service.checksum, 0, 255)
    assert header == service.data[:256]
    assert service.calls[1].headers["Range"] == "bytes=10-255"


@pytest.mark.parametrize(
    "headers,error",
    [
        ({"ETag": '"wrong"'}, ArchiveChanged),
        ({"Content-Range": "bytes 1-255/9999"}, ProtocolError),
    ],
)
def test_range_validation(service, headers, error):
    service.failures = [
        response(
            206,
            b"",
            {
                "ETag": f'"{service.checksum}"',
                "Content-Range": f"bytes 0-255/{len(service.data)}",
                **headers,
            },
        )
    ]
    with Transport(Options.parse({}, {})) as transport:
        with pytest.raises(error):
            transport.range("seg_0000000000.jss", service.checksum, 0, 255)


def test_private_token_only_in_authorization(service):
    with Transport(Options.parse({"api_key": "supersecret"}, {})) as transport:
        transport.plan(0)
    request = service.calls[0]
    assert request.headers["Authorization"] == "Bearer supersecret"
    assert "supersecret" not in request.url
    assert "supersecret" not in request.body.decode()
