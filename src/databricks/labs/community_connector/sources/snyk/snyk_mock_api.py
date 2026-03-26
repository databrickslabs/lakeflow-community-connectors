"""In-memory mock Snyk API for demo and testing without real API access.

Exposes a ``SnykMockSession`` that behaves like ``requests.Session`` — the
connector calls ``.get(url, params=..., timeout=...)`` and receives a response
object with ``.status_code``, ``.json()``, and ``.raise_for_status()``.

Endpoints simulated
-------------------
GET /v1/orgs
    Returns a list of orgs (V1 format: ``{"orgs": [...]}``)

GET /rest/orgs/{org_id}/projects
GET /rest/orgs/{org_id}/issues          supports ``updated_after`` filter
GET /rest/orgs/{org_id}/targets
GET /rest/orgs/{org_id}/users
GET /rest/orgs/{org_id}/vulnerabilities
    All REST endpoints return JSON:API format with cursor-based pagination
    via ``links.next``.  The mock uses ``starting_after`` as a numeric offset
    into the in-memory list.

Pagination
----------
The mock sets ``_PAGE_SIZE = 3`` intentionally small so that multi-page
fetches are exercised in tests even with small seed datasets.

Singleton
---------
Use ``get_mock_api()`` to obtain the shared instance.  Call ``reset_mock_api()``
at the start of each test run to get a fresh copy (same pattern as the example
connector's ``reset_api``).

Usage in connector
------------------
When ``options["token"] == "mock"``, ``SnykLakeflowConnect.__init__`` replaces
the real ``requests.Session`` with ``get_mock_api().get_session()``.
"""

from __future__ import annotations

import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import parse_qs, urlparse

MOCK_ORG_ID = "org-mock-0001"

_PAGE_SIZE = 3  # intentionally small to exercise multi-page pagination in tests


# ── helpers ──────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


# ── response / session ───────────────────────────────────────────────────────

class SnykMockResponse:
    """Mimics ``requests.Response`` with ``.status_code``, ``.json()``, and ``.raise_for_status()``."""

    __slots__ = ("status_code", "_body")

    def __init__(self, status_code: int, body) -> None:
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}: {self._body}")


class SnykMockSession:
    """Drop-in replacement for ``requests.Session`` backed by ``SnykMockAPI``.

    Parses the full URL (including embedded query params from ``links.next``
    follow-up requests) and delegates to the API instance.
    """

    def __init__(self, api: "SnykMockAPI") -> None:
        self.headers: dict = {}
        self._api = api

    def get(self, url: str, params=None, timeout=None) -> SnykMockResponse:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        flat_qs: dict = {k: v[0] for k, v in qs.items()}
        if params:
            flat_qs.update({k: str(v) for k, v in params.items()})
        return self._api.handle_get(parsed.path, flat_qs)


# ── mock API ─────────────────────────────────────────────────────────────────

class SnykMockAPI:
    """In-memory Snyk API with seeded data for all five tables.

    Tables seeded
    ~~~~~~~~~~~~~
    organizations  1 org
    projects       5 projects  (pagination: ceil(5/3) = 2 pages)
    issues        10 issues    (CDC via ``updated_at``; supports ``updated_after``)
    targets        5 targets
    users          5 users
    vulnerabilities 10 vulnerabilities
    """

    def __init__(self) -> None:
        base = _now() - timedelta(hours=2)

        # ── organizations (V1 format) ─────────────────────────────────────
        self._orgs = [
            {
                "id": MOCK_ORG_ID,
                "name": "Acme Security",
                "slug": "acme-security",
                "url": "https://app.snyk.io/org/acme-security",
            }
        ]

        # ── projects ─────────────────────────────────────────────────────
        self._projects = [
            self._jsonapi(
                f"proj-{i:04d}", "project",
                {
                    "name": f"acme/service-{i}",
                    "status": "active",
                    "origin": "github",
                    "created": _iso(base + timedelta(hours=i)),
                },
                {"organization": {"data": {"id": MOCK_ORG_ID}}},
            )
            for i in range(5)
        ]

        # ── issues ───────────────────────────────────────────────────────
        self._issues = [
            self._jsonapi(
                str(uuid.UUID(int=i)), "issue",
                {
                    "title": f"Security issue {i}",
                    "status": "open",
                    "effective_severity_level": ["low", "medium", "high", "critical"][i % 4],
                    "ignored": False,
                    "created_at": _iso(base + timedelta(minutes=i * 5)),
                    "updated_at": _iso(base + timedelta(minutes=i * 5)),
                    "tool": ["snyk-open-source", "snyk-code"][i % 2],
                },
                {
                    "organization": {"data": {"id": MOCK_ORG_ID}},
                    "scan_item": {"data": {"id": f"proj-{i % 5:04d}"}},
                },
            )
            for i in range(10)
        ]

        # ── targets ──────────────────────────────────────────────────────
        self._targets = [
            self._jsonapi(
                f"target-{i:04d}", "target",
                {
                    "display_name": f"github.com/acme/repo-{i}",
                    "url": f"https://github.com/acme/repo-{i}",
                },
                {"organization": {"data": {"id": MOCK_ORG_ID}}},
            )
            for i in range(5)
        ]

        # ── users ─────────────────────────────────────────────────────────
        self._users = [
            self._jsonapi(
                f"user-{i:04d}", "user",
                {
                    "name": f"User {i}",
                    "email": f"user{i}@acme.io",
                    "username": f"user{i}",
                },
                {"organization": {"data": {"id": MOCK_ORG_ID}}},
            )
            for i in range(5)
        ]

        # ── vulnerabilities ───────────────────────────────────────────────
        _packages = ["lodash", "axios", "express", "moment", "requests"]
        _severities = ["low", "medium", "high", "critical"]
        self._vulnerabilities = [
            self._jsonapi(
                f"vuln-{i:04d}", "vulnerability",
                {
                    "title": f"Vulnerability in {_packages[i % 5]}",
                    "severity": _severities[i % 4],
                    "status": "open",
                    "cve": f"CVE-2024-{1000 + i}",
                    "cvss_score": round(3.0 + i * 0.6, 1),
                    "package": _packages[i % 5],
                    "version": "1.0.0",
                    "fixed_in": "1.0.1",
                    "disclosure_time": _iso(base + timedelta(days=i)),
                    "publication_time": _iso(base + timedelta(days=i, hours=1)),
                },
                {
                    "organization": {"data": {"id": MOCK_ORG_ID}},
                    "project": {"data": {"id": f"proj-{i % 5:04d}"}},
                },
            )
            for i in range(10)
        ]

    # ── static helpers ────────────────────────────────────────────────────

    @staticmethod
    def _jsonapi(id_: str, type_: str, attributes: dict, relationships: dict) -> dict:
        """Wrap fields in JSON:API envelope."""
        return {
            "id": id_,
            "type": type_,
            "attributes": attributes,
            "relationships": relationships,
        }

    # ── pagination ────────────────────────────────────────────────────────

    def _paginate(self, path: str, records: list, params: dict) -> SnykMockResponse:
        """Return one page of records with a ``links.next`` cursor if more remain."""
        offset = int(params.get("starting_after", 0))
        limit = min(int(params.get("limit", _PAGE_SIZE)), _PAGE_SIZE)
        page = records[offset: offset + limit]
        links: dict = {}
        next_offset = offset + limit
        if next_offset < len(records):
            version = params.get("version", "2024-10-15")
            links["next"] = f"{path}?version={version}&limit={limit}&starting_after={next_offset}"
        return SnykMockResponse(200, {"data": page, "links": links})

    # ── request routing ───────────────────────────────────────────────────

    def handle_get(self, path: str, params: dict) -> SnykMockResponse:
        """Route a GET request and return the appropriate mock response."""
        # V1: GET /v1/orgs
        if path.rstrip("/") == "/v1/orgs":
            return SnykMockResponse(200, {"orgs": self._orgs})

        # REST: GET /rest/orgs/{org_id}/{resource}
        parts = [p for p in path.split("/") if p]
        # ["rest", "orgs", "{org_id}", "{resource}"]
        if len(parts) >= 4 and parts[0] == "rest" and parts[1] == "orgs":
            org_id = parts[2]
            resource = parts[3]

            if org_id != MOCK_ORG_ID:
                return SnykMockResponse(404, {"error": f"Org '{org_id}' not found"})

            resource_map: dict[str, list] = {
                "projects": self._projects,
                "issues": self._issues,
                "targets": self._targets,
                "users": self._users,
                "vulnerabilities": self._vulnerabilities,
            }
            if resource not in resource_map:
                return SnykMockResponse(404, {"error": f"Unknown resource: '{resource}'"})

            records = resource_map[resource]

            # issues supports updated_after filter for CDC
            if resource == "issues" and params.get("updated_after"):
                cutoff = params["updated_after"]
                records = [
                    r for r in records
                    if r["attributes"].get("updated_at", "") > cutoff
                ]

            return self._paginate(path, records, params)

        return SnykMockResponse(404, {"error": f"No route for GET {path}"})

    def get_session(self) -> SnykMockSession:
        """Return a new session backed by this API instance."""
        return SnykMockSession(self)


# ── singleton management ──────────────────────────────────────────────────────

_INSTANCE: Optional[SnykMockAPI] = None
_INSTANCE_LOCK = threading.Lock()


def get_mock_api() -> SnykMockAPI:
    """Return the singleton mock API instance, creating it on first call."""
    global _INSTANCE  # noqa: PLW0603
    with _INSTANCE_LOCK:
        if _INSTANCE is None:
            _INSTANCE = SnykMockAPI()
        return _INSTANCE


def reset_mock_api() -> SnykMockAPI:
    """Reset the singleton — call at the start of each test run."""
    global _INSTANCE  # noqa: PLW0603
    with _INSTANCE_LOCK:
        _INSTANCE = SnykMockAPI()
        return _INSTANCE
