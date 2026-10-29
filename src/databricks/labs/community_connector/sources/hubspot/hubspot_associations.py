"""Complete association batch reads with independent per-parent pagination.

The injected POST callback owns HTTP status handling, throttling and 429 retries.
This helper reconciles the returned protocol payload and never returns partial data.
"""

import time
from typing import Callable
from urllib.parse import quote

ASSOCIATION_BATCH_READ_SIZE = 1000
ASSOCIATION_MIN_INTERVAL_S = 0.11
ASSOCIATION_MAX_RETRIES = 5
ASSOCIATION_MAX_BACKOFF_S = 30.0
ASSOCIATION_SAFE_ERROR_LABELS = frozenset({
    "VALIDATION_ERROR", "OBJECT_NOT_FOUND", "MISSING_SCOPES",
    "INVALID_AUTHENTICATION", "RATE_LIMITS", "INTERNAL_ERROR",
    "FORBIDDEN", "CONFLICT",
})


def _association_error_labels(errors: list) -> tuple[str, str]:
    """Emit only fixed diagnostic labels, never arbitrary API error text."""
    categories, codes = set(), set()

    def label(value):
        return value if isinstance(value, str) and value in ASSOCIATION_SAFE_ERROR_LABELS else "UNKNOWN"

    for error in errors:
        if not isinstance(error, dict):
            categories.add("UNKNOWN")
            continue
        if "category" in error:
            categories.add(label(error["category"]))
        details = error.get("errors", [])
        if not isinstance(details, list):
            codes.add("UNKNOWN")
            continue
        for detail in details:
            if isinstance(detail, dict) and "code" in detail:
                codes.add(label(detail["code"]))
            elif not isinstance(detail, dict):
                codes.add("UNKNOWN")
    return "|".join(sorted(categories)) or "NONE", "|".join(sorted(codes)) or "NONE"


def _association_identifier(value, kind: str) -> str:
    # HubSpot may encode object IDs as JSON numbers or strings. Never coerce
    # booleans, nulls, containers or fractional numbers into plausible IDs.
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return str(value)
    if isinstance(value, str) and value and value.strip() == value:
        return value
    raise RuntimeError(f"Malformed HubSpot association {kind}")


def _association_next_cursor(result: dict):
    if "paging" not in result:
        return None
    paging = result["paging"]
    if not isinstance(paging, dict):
        raise RuntimeError("Malformed HubSpot association paging")
    if "next" not in paging:
        return None
    next_page = paging["next"]
    if not isinstance(next_page, dict) or "after" not in next_page:
        raise RuntimeError("Malformed HubSpot association next page")
    return _association_identifier(next_page["after"], "cursor")


class HubspotAssociations:
    """Read all associations for each requested parent, or raise."""

    def __init__(
        self,
        base_url: str,
        post: Callable[[str, str, dict, float], dict],
        sleep: Callable[[float], None] = time.sleep,
        *,
        get: Callable[[str, str, dict, float], dict] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._post = post
        self._sleep = sleep
        self._get = get

    def read(self, from_type: str, to_type: str, parent_ids: list[str]) -> dict[str, list[str]]:
        """Return sorted unique target IDs; retry omissions at their current cursor.

        Five additional attempts are allowed for each unresolved parent/page.
        Successful pages reset that budget, so legitimate page counts are unlimited.
        Non-COMPLETE batches are validated but their results are not committed.
        """
        parents = [_association_identifier(parent, "requested parent ID") for parent in parent_ids]
        if len(set(parents)) != len(parents):
            raise RuntimeError("Duplicate HubSpot association requested parent ID")
        targets = {parent: set() for parent in parents}
        url = (
            f"{self.base_url}/crm/associations/2026-09/"
            f"{from_type}/{to_type}/batch/read"
        )
        for start in range(0, len(parents), ASSOCIATION_BATCH_READ_SIZE):
            pending = dict.fromkeys(parents[start : start + ASSOCIATION_BATCH_READ_SIZE])
            retries = dict.fromkeys(pending, 0)
            seen = {parent: set() for parent in pending}
            while pending:
                inputs = [
                    {"id": parent, **({"after": cursor} if cursor is not None else {})}
                    for parent, cursor in pending.items()
                ]
                data = self._post("associations", url, {"inputs": inputs}, ASSOCIATION_MIN_INTERVAL_S)
                results = self._validate_results(data, pending)
                errors = data.get("errors", [])
                error_count = data.get("numErrors", 0)
                if (not isinstance(errors, list) or isinstance(error_count, bool)
                        or not isinstance(error_count, int) or error_count < 0):
                    raise RuntimeError("Malformed HubSpot association error envelope")
                # Error context has no universal parent-ID key. Do not commit
                # results from an errored batch without reliable attribution.
                has_errors = bool(errors) or error_count > 0
                unresolved = []
                for parent in list(pending):
                    if has_errors or data.get("status") != "COMPLETE" or parent not in results:
                        if retries[parent] >= ASSOCIATION_MAX_RETRIES:
                            if self._get is not None and self._can_reconcile_not_found(data):
                                # GET and batch cursors are not interchangeable.
                                # Complete fresh reads before replacing any old pages.
                                reconciled = {
                                    item: self._read_one(from_type, to_type, item)
                                    for item in pending
                                }
                                targets.update(reconciled)
                                pending.clear()
                                break
                            status = data.get("status")
                            if not isinstance(status, str) or status not in {
                                "COMPLETE", "PENDING", "PROCESSING", "CANCELED"
                            }:
                                status = "MISSING" if status is None else "UNKNOWN"
                            errors = data.get("errors", [])
                            error_count = len(errors) if isinstance(errors, list) else "malformed"
                            categories, codes = _association_error_labels(errors)
                            raise RuntimeError(
                                "Incomplete HubSpot associations: exhausted retries "
                                f"(from_type={from_type}, to_type={to_type}, "
                                f"status={status}, requested={len(pending)}, "
                                f"returned={len(results)}, errors={error_count}, "
                                f"numErrors={data.get('numErrors', 0)}, "
                                f"error_categories={categories}, error_codes={codes})"
                            )
                        retries[parent] += 1
                        unresolved.append(retries[parent])
                        continue
                    ids, cursor = results[parent]
                    if cursor is not None and cursor in seen[parent]:
                        raise RuntimeError("Repeated or cyclic HubSpot association cursor")
                    targets[parent].update(ids)
                    if cursor is None:
                        del pending[parent]
                    else:
                        seen[parent].add(cursor)
                        pending[parent] = cursor
                        retries[parent] = 0
                if unresolved:
                    self._sleep(min(2.0 ** (max(unresolved) - 1), ASSOCIATION_MAX_BACKOFF_S))
        return {parent: sorted(ids) for parent, ids in targets.items()}

    @staticmethod
    def _can_reconcile_not_found(data: dict) -> bool:
        errors = data.get("errors")
        count = data.get("numErrors")
        return (
            data.get("status") == "COMPLETE"
            and isinstance(data.get("results"), list)
            and isinstance(errors, list) and bool(errors)
            and type(count) is int and count == len(errors)
            and all(isinstance(error, dict) and error.get("category") == "OBJECT_NOT_FOUND"
                    for error in errors)
        )

    def _read_one(self, from_type: str, to_type: str, parent: str) -> set[str]:
        base = f"{self.base_url}/crm/objects/2026-09/{quote(from_type, safe='')}/{quote(parent, safe='')}"
        url = f"{base}/associations/{quote(to_type, safe='')}"
        targets, seen = set(), set()
        cursor = None
        while True:
            params = {"limit": 500, **({"after": cursor} if cursor is not None else {})}
            data = self._get("associations", url, params, ASSOCIATION_MIN_INTERVAL_S)
            if (not isinstance(data, dict) or not isinstance(data.get("results"), list)
                    or any(key in data for key in ("errors", "numErrors", "category"))
                    or data.get("status", "COMPLETE") != "COMPLETE"):
                raise RuntimeError("Malformed HubSpot association GET collection")
            for result in data["results"]:
                if not isinstance(result, dict) or not isinstance(result.get("associationTypes"), list):
                    raise RuntimeError("Malformed HubSpot association GET target")
                for kind in result["associationTypes"]:
                    if (not isinstance(kind, dict) or type(kind.get("typeId")) is not int
                            or kind["typeId"] < 0 or not isinstance(kind.get("category"), str)
                            or kind["category"] not in {
                                "HUBSPOT_DEFINED", "INTEGRATOR_DEFINED", "USER_DEFINED", "WORK"
                            }):
                        raise RuntimeError("Malformed HubSpot association GET type")
                targets.add(_association_identifier(result.get("toObjectId"), "GET target ID"))
            cursor = _association_next_cursor(data)
            if cursor is None:
                break
            if cursor in seen:
                raise RuntimeError("Repeated or cyclic HubSpot association GET cursor")
            seen.add(cursor)
        if not targets:
            # An empty collection cannot establish that the parent still exists.
            obj = self._get("associations", base, {"properties": "hs_object_id", "archived": "false"},
                            ASSOCIATION_MIN_INTERVAL_S)
            if (not isinstance(obj, dict) or obj.get("archived") is not False
                    or any(key in obj for key in ("errors", "numErrors", "category"))
                    or obj.get("status", "COMPLETE") != "COMPLETE"
                    or _association_identifier(obj.get("id"), "GET parent ID") != parent):
                raise RuntimeError("Unproven HubSpot association GET parent identity")
        return targets

    @staticmethod
    def _validate_results(data, pending: dict) -> dict:
        if not isinstance(data, dict):
            raise RuntimeError("Malformed HubSpot association batch")
        results = data.get("results", [])
        if not isinstance(results, list):
            raise RuntimeError("Malformed HubSpot association results")
        parsed = {}
        for result in results:
            if not isinstance(result, dict) or not isinstance(result.get("from"), dict):
                raise RuntimeError("Malformed HubSpot association parent")
            parent = _association_identifier(result["from"].get("id"), "parent ID")
            if parent not in pending or parent in parsed:
                raise RuntimeError("Unrequested or duplicate HubSpot association parent")
            if not isinstance(result.get("to"), list):
                raise RuntimeError("Malformed HubSpot association target list")
            ids = set()
            for target in result["to"]:
                if not isinstance(target, dict):
                    raise RuntimeError("Malformed HubSpot association target")
                ids.add(_association_identifier(target.get("toObjectId"), "target ID"))
            parsed[parent] = (ids, _association_next_cursor(result))
        return parsed
