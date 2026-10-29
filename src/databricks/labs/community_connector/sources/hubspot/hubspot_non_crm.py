"""Complete snapshots for HubSpot resources outside CRM object reads.

HTTP failures and malformed pages abort the snapshot. The caller owns publication
and offsets; no cross-read cache or incremental cursor is retained here.
"""

import json
import time
from typing import Callable
from urllib.parse import quote

import requests
from pyspark.sql.types import (
    ArrayType, BooleanType, LongType, MapType, StringType, StructField, StructType,
)

NON_CRM_TABLES = (
    "owners", "pipelines", "forms", "contact_lists",
    "contact_list_memberships", "form_submissions",
)
NON_CRM_MAX_RETRIES = 5
NON_CRM_TIMEOUT_S = 60
NON_CRM_MIN_INTERVAL_S = 0.11
NON_CRM_PIPELINE_TYPES = {
    "0-3", "0-5", "0-27", "0-421", "0-410", "0-136", "0-420",
    "0-123", "0-970", "0-162", "0-115",
}
NON_CRM_KEYS = {
    "owners": ["id"], "pipelines": ["objectTypeId", "id"], "forms": ["id"],
    "contact_lists": ["listId"], "contact_list_memberships": ["listId", "recordId"],
    "form_submissions": ["formId", "conversionId"],
}


def _non_crm_id(value):
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return str(value)
    if isinstance(value, str) and value and value.strip() == value:
        return value
    raise RuntimeError("Malformed HubSpot non-CRM identity or cursor")


def _non_crm_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _non_crm_after(data):
    if "paging" not in data:
        return None
    paging = data["paging"]
    if not isinstance(paging, dict):
        raise RuntimeError("Malformed HubSpot non-CRM paging")
    if "next" not in paging:
        return None
    nxt = paging["next"]
    if not isinstance(nxt, dict) or "after" not in nxt:
        raise RuntimeError("Malformed HubSpot non-CRM next page")
    return _non_crm_id(nxt["after"])


def _non_crm_with_identity(source, field, identity):
    if field in source:
        if _non_crm_id(source[field]) != identity:
            raise RuntimeError("Conflicting HubSpot non-CRM partition identity")
        return dict(source)
    return {**source, field: identity}


def _non_crm_fields(names, dtype=None):
    return [StructField(name, dtype or StringType(), True) for name in names]


class HubspotNonCrm:
    """Read all partitions and pages before returning a snapshot."""

    def __init__(self, base_url, auth_header, custom_types: Callable, post: Callable,
                 sleep: Callable = time.sleep):
        self.base_url = base_url.rstrip("/")
        self.auth_header = auth_header
        self._custom_types = custom_types
        self._post = post
        self._sleep = sleep
        self._last_get = None

    @staticmethod
    def list_tables() -> list[str]:
        return list(NON_CRM_TABLES)

    @staticmethod
    def read_table_metadata(table) -> dict:
        if table not in NON_CRM_KEYS:
            raise ValueError("Unknown HubSpot non-CRM table")
        return {"primary_keys": list(NON_CRM_KEYS[table]), "cursor_field": None,
                "ingestion_type": "snapshot"}

    @staticmethod
    def get_table_schema(table) -> StructType:
        HubspotNonCrm.read_table_metadata(table)
        team = StructType(_non_crm_fields(["id", "name"]) +
                          _non_crm_fields(["primary"], BooleanType()))
        stage = StructType(_non_crm_fields(["id", "label", "createdAt", "updatedAt"]) +
                           _non_crm_fields(["displayOrder"], LongType()) +
                           _non_crm_fields(["metadata"], MapType(StringType(), StringType())))
        value = StructType(_non_crm_fields(["name", "value"]))
        fields = {
            "owners": _non_crm_fields([
                "id", "email", "firstName", "lastName", "type", "userId",
                "userIdIncludingInactive", "createdAt", "updatedAt",
            ]) + _non_crm_fields(["archived"], BooleanType()) +
                _non_crm_fields(["teams"], ArrayType(team)),
            "pipelines": _non_crm_fields([
                "objectTypeId", "id", "label", "createdAt", "updatedAt",
            ]) + _non_crm_fields(["displayOrder"], LongType()) +
                _non_crm_fields(["archived"], BooleanType()) +
                _non_crm_fields(["stages"], ArrayType(stage)),
            "forms": _non_crm_fields([
                "id", "name", "formType", "createdAt", "updatedAt", "archivedAt",
                "configuration", "displayOptions", "fieldGroups", "legalConsentOptions",
            ]) + _non_crm_fields(["archived"], BooleanType()),
            "contact_lists": _non_crm_fields([
                "listId", "name", "objectTypeId", "processingType", "processingStatus",
                "createdAt", "updatedAt", "additionalProperties",
            ]) + _non_crm_fields(["listVersion"], LongType()),
            "contact_list_memberships": _non_crm_fields([
                "listId", "recordId", "membershipTimestamp",
            ]),
            "form_submissions": _non_crm_fields(["formId", "conversionId", "pageUrl"]) +
                _non_crm_fields(["submittedAt"], LongType()) +
                _non_crm_fields(["values"], ArrayType(value)),
        }
        return StructType(fields[table] + _non_crm_fields(["payload"]))

    def _get(self, route, params):
        for attempt in range(NON_CRM_MAX_RETRIES + 1):
            now = time.monotonic()
            if self._last_get is not None:
                delay = NON_CRM_MIN_INTERVAL_S - (now - self._last_get)
                if delay > 0:
                    self._sleep(delay)
            self._last_get = time.monotonic()
            response = requests.get(self.base_url + route, headers=self.auth_header,
                                    params=params, timeout=NON_CRM_TIMEOUT_S)
            if response.status_code == 200:
                return response.json()
            if response.status_code == 429 and attempt < NON_CRM_MAX_RETRIES:
                try:
                    delay = float(response.headers.get("Retry-After", 2 ** attempt))
                    if not 0 <= delay <= 30:
                        delay = min(2 ** attempt, 30)
                except (ValueError, TypeError):
                    delay = min(2 ** attempt, 30)
                self._sleep(delay)
                continue
            # Do not leak response bodies, tokens, or identifiers into errors.
            raise RuntimeError(f"HubSpot non-CRM HTTP {response.status_code}")
        raise RuntimeError("HubSpot non-CRM retries exhausted")

    def _pages(self, route, params=None, body=None):
        cursor = None
        seen = set()
        rows = []
        while True:
            arguments = dict(body if body is not None else (params or {}))
            if cursor is not None:
                arguments["after"] = cursor
            if body is None:
                data = self._get(route, arguments)
            else:
                data = self._post("non_crm", self.base_url + route, arguments,
                                  NON_CRM_MIN_INTERVAL_S)
            if not isinstance(data, dict) or not isinstance(data.get("results"), list):
                raise RuntimeError("Malformed HubSpot non-CRM results")
            if any(not isinstance(row, dict) for row in data["results"]):
                raise RuntimeError("Malformed HubSpot non-CRM result")
            if data.get("errors") or data.get("status") in {"PENDING", "PROCESSING", "CANCELED"}:
                raise RuntimeError("Incomplete HubSpot non-CRM page")
            rows.extend(data["results"])
            cursor = _non_crm_after(data)
            if cursor is None:
                return rows
            if cursor in seen:
                raise RuntimeError("Repeated or cyclic HubSpot non-CRM cursor")
            seen.add(cursor)

    def _inventory(self, table):
        if table in {"owners", "forms"}:
            route = "/crm/owners/2026-09" if table == "owners" else "/marketing/v3/forms"
            rows = []
            for archived in (False, True):
                params = {"archived": str(archived).lower(), "limit": 100}
                if table == "forms":
                    params["formTypes"] = "all"
                for source in self._pages(route, params):
                    row = dict(source)
                    row.setdefault("archived", archived)
                    rows.append(row)
            return rows
        rows = self._pages("/crm/lists/2026-09/all", body={
            "objectTypeId": "0-1", "processingTypes": ["MANUAL", "DYNAMIC", "SNAPSHOT"],
            "additionalProperties": [], "count": 500,
        })
        for row in rows:
            if row.get("objectTypeId") != "0-1":
                raise RuntimeError("Non-contact list in HubSpot contact-list inventory")
        return rows

    def read_table(self, table, table_options=None) -> list[dict]:
        self.read_table_metadata(table)
        if table in {"owners", "forms", "contact_lists"}:
            sources = self._inventory(table)
        elif table == "pipelines":
            options = table_options or {}
            if "pipeline_object_types" in options:
                types = options["pipeline_object_types"].split(",")
                types = [value.strip() for value in types]
            else:
                types = ["0-3", "0-5", "0-27", *self._custom_types()]
            sources = []
            for object_type in dict.fromkeys(types):
                object_type = _non_crm_id(object_type)
                if object_type not in NON_CRM_PIPELINE_TYPES and not (
                    object_type.startswith("2-") and object_type[2:].isdigit()
                ):
                    raise ValueError("Unsupported HubSpot pipeline object type")
                for source in self._pages(f"/crm/pipelines/2026-09/{object_type}"):
                    sources.append(_non_crm_with_identity(source, "objectTypeId", object_type))
        else:
            inventory_table = "forms" if table == "form_submissions" else "contact_lists"
            parents = self.read_table(inventory_table, {})
            sources = []
            for parent in parents:
                if table == "contact_list_memberships":
                    if parent.get("processingStatus") == "PROCESSING":
                        raise RuntimeError("HubSpot list membership is still PROCESSING")
                    identity = parent["listId"]
                    route = f"/crm/lists/2026-09/{quote(identity, safe='')}/memberships"
                    field, limit = "listId", 100
                else:
                    identity = parent["id"]
                    route = f"/form-integrations/v1/submissions/forms/{quote(identity, safe='')}"
                    field, limit = "formId", 50
                for source in self._pages(route, {"limit": limit}):
                    sources.append(_non_crm_with_identity(source, field, identity))
        output = {}
        for source in sources:
            row = self._row(table, source)
            identity = tuple(row[key] for key in NON_CRM_KEYS[table])
            if identity in output and output[identity] != row:
                raise RuntimeError("Conflicting duplicate HubSpot non-CRM identity")
            output[identity] = row
        return list(output.values())

    def _row(self, table, source):
        row = {field.name: source.get(field.name) for field in self.get_table_schema(table)}
        for key in NON_CRM_KEYS[table]:
            row[key] = _non_crm_id(source.get(key))
        for key in ("userId", "userIdIncludingInactive"):
            if key in row and row[key] is not None:
                row[key] = _non_crm_id(row[key])
        for key in ("configuration", "displayOptions", "fieldGroups",
                    "legalConsentOptions", "additionalProperties"):
            if key in source:
                row[key] = _non_crm_json(source[key])
        for key in ("teams", "stages", "values"):
            if key not in row or row[key] is None:
                continue
            if not isinstance(row[key], list) or any(not isinstance(v, dict) for v in row[key]):
                raise RuntimeError("Malformed HubSpot non-CRM nested array")
            nested = []
            for value in row[key]:
                value = dict(value)
                if key in {"teams", "stages"}:
                    value["id"] = _non_crm_id(value.get("id"))
                if key == "stages" and value.get("metadata") is not None:
                    if not isinstance(value["metadata"], dict):
                        raise RuntimeError("Malformed HubSpot pipeline stage metadata")
                    value["metadata"] = {
                        k: v if isinstance(v, str) else _non_crm_json(v)
                        for k, v in value["metadata"].items()
                    }
                nested.append(value)
            row[key] = nested
        row["payload"] = _non_crm_json(source)
        return row
