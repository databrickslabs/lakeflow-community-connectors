from typing import Iterator
from pyspark.sql.types import StructType
from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.snyk.snyk_schemas import (
    TABLE_SCHEMAS, TABLE_METADATA, SUPPORTED_TABLES
)

_API_VERSION = "2024-10-15"
_REST_BASE = "https://api.snyk.io/rest"
_V1_BASE = "https://api.snyk.io/v1"


class SnykLakeflowConnect(LakeflowConnect):

    def __init__(self, options: dict[str, str]) -> None:
        token = options.get("token")
        if not token:
            raise ValueError("Snyk connector requires 'token' in options")

        if token == "mock":
            from databricks.labs.community_connector.sources.snyk.snyk_mock_api import get_mock_api
            self._session = get_mock_api().get_session()
        else:
            import requests
            self._session = requests.Session()
            self._session.headers.update({
                "Authorization": f"token {token}",
                "Accept": "application/vnd.api+json",
            })

        self._rest_base = options.get("api_base_url", _REST_BASE).rstrip("/")
        self._v1_base = options.get("v1_base_url", _V1_BASE).rstrip("/")

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES.copy()

    def get_table_schema(self, table_name: str, table_options: dict) -> StructType:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict) -> dict:
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict
    ) -> tuple[Iterator[dict], dict]:
        dispatch = {
            "organizations": self._read_organizations,
            "projects": self._read_projects,
            "issues": self._read_issues,
            "targets": self._read_targets,
            "users": self._read_users,
            "vulnerabilities": self._read_vulnerabilities,
        }
        if table_name not in dispatch:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return dispatch[table_name](start_offset, table_options)

    def _paginate_rest(self, url: str, params: dict) -> list[dict]:
        """Paginate Snyk REST (JSON:API) using links.next cursor."""
        results = []
        next_url = url
        next_params: dict | None = params

        while next_url:
            resp = self._session.get(next_url, params=next_params, timeout=30)
            if resp.status_code == 429:
                # Rate-limited — wait and retry once
                import time; time.sleep(60)
                resp = self._session.get(next_url, params=next_params, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            data = body.get("data", [])
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict):
                results.append(data)
            links = body.get("links", {})
            next_path = links.get("next")
            if next_path:
                next_url = f"https://api.snyk.io{next_path}"
                next_params = None  # cursor already embedded in next_path
            else:
                break
        return results

    def _flatten_jsonapi(self, record: dict) -> dict:
        """Flatten JSON:API { id, type, attributes, relationships } to flat dict."""
        flat = {"id": record.get("id"), "type": record.get("type")}
        flat.update(record.get("attributes", {}))
        for rel_name, rel_data in (record.get("relationships") or {}).items():
            inner = rel_data.get("data") or {}
            if isinstance(inner, dict):
                flat[f"{rel_name}_id"] = inner.get("id")
        return flat

    def _read_organizations(self, start_offset: dict, table_options: dict):
        url = f"{self._v1_base}/orgs"
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()
        orgs_raw = resp.json().get("orgs", [])
        records = [
            {"id": o.get("id"), "name": o.get("name"),
             "slug": o.get("slug"), "url": o.get("url")}
            for o in orgs_raw
        ]
        return iter(records), {}

    def _require_org_id(self, table_options: dict, table_name: str) -> str:
        org_id = table_options.get("org_id")
        if not org_id:
            raise ValueError(
                f"Table '{table_name}' requires 'org_id' in table_options. "
                "Add it to table_configuration in the pipeline spec."
            )
        return org_id

    def _read_projects(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "projects")
        url = f"{self._rest_base}/orgs/{org_id}/projects"
        params = {"version": _API_VERSION, "limit": 100}
        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]
        return iter(records), {}

    def _read_issues(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "issues")
        cursor_dt = (start_offset or {}).get("cursor")
        url = f"{self._rest_base}/orgs/{org_id}/issues"
        params: dict = {"version": _API_VERSION, "limit": 100}
        if cursor_dt:
            params["updated_after"] = cursor_dt

        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]

        if not records:
            return iter([]), start_offset or {}

        max_updated = max(
            r["updated_at"] for r in records if r.get("updated_at")
        )
        new_offset = {"cursor": max_updated}
        return iter(records), new_offset if new_offset != start_offset else start_offset or {}

    def _read_targets(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "targets")
        url = f"{self._rest_base}/orgs/{org_id}/targets"
        params = {"version": _API_VERSION, "limit": 100}
        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]
        return iter(records), {}

    def _read_users(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "users")
        url = f"{self._rest_base}/orgs/{org_id}/users"
        params = {"version": _API_VERSION, "limit": 100}
        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]
        return iter(records), {}

    def _read_vulnerabilities(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "vulnerabilities")
        url = f"{self._rest_base}/orgs/{org_id}/vulnerabilities"
        params = {"version": _API_VERSION, "limit": 100}
        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]
        return iter(records), {}