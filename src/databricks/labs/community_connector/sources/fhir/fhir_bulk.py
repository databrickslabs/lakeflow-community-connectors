"""FHIR Bulk Data Export client (HL7 Bulk Data Access IG).

Implements the async $export workflow:
  1. Kick-off:  GET [base]/$export  ->  202 + Content-Location
  2. Poll:      GET [status-url]    ->  202 (in-progress) | 200 (manifest)
  3. Download:  GET [file-url]      ->  NDJSON stream
  4. Cleanup:   DELETE [status-url]  ->  202 (best-effort)

Supports all three export scopes (system, patient, group) and handles
server-specific download patterns: bearer-token auth, presigned URLs,
and 307 redirects to presigned S3 (Cerner).
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote, urljoin, urlparse

import requests

from databricks.labs.community_connector.sources.fhir.fhir_constants import (
    BULK_EXPORT_TIMEOUT,
    BULK_MAX_REDIRECTS,
    BULK_MIN_POLL_SLEEP,
    BULK_NDJSON_TIMEOUT,
    BULK_POLL_INTERVAL,
    BULK_VALID_SCOPES,
    HTTP_TIMEOUT,
    INITIAL_BACKOFF,
    MAX_RETRIES,
    MAX_RETRY_AFTER,
    RETRIABLE_STATUS_CODES,
    UNTYPED_BUCKET,
)
from databricks.labs.community_connector.sources.fhir.fhir_utils import (
    SmartAuthClient,
    auth_headers,
    retry_after as _retry_after,
)

logger = logging.getLogger(__name__)

NDJSON = "application/fhir+ndjson"


class BulkExportClient:
    """Client for the FHIR Bulk Data Access IG async export workflow."""

    def __init__(
        self,
        base_url: str,
        auth_client: SmartAuthClient,
        scope: str = "system",
        group_id: Optional[str] = None,
        poll_interval: int = BULK_POLL_INTERVAL,
        timeout: int = BULK_EXPORT_TIMEOUT,
        ignore_errors: bool = False,
    ) -> None:
        if scope not in BULK_VALID_SCOPES:
            raise ValueError(
                f"Invalid bulk_export_scope {scope!r}. Use one of: {sorted(BULK_VALID_SCOPES)}."
            )
        if scope == "group" and not group_id:
            raise ValueError("bulk_export_group_id is required when bulk_export_scope is 'group'.")
        self._base_url = base_url.rstrip("/")
        self._auth_client = auth_client
        self._scope = scope
        self._group_id = group_id
        self._poll_interval = poll_interval
        self._timeout = timeout
        self._ignore_errors = ignore_errors
        self._session = requests.Session()

    def _export_url(self) -> str:
        if self._scope == "patient":
            return f"{self._base_url}/Patient/$export"
        if self._scope == "group":
            safe_id = quote(self._group_id, safe="")
            return f"{self._base_url}/Group/{safe_id}/$export"
        return f"{self._base_url}/$export"

    def export(
        self,
        resource_types: list,
        since: Optional[str] = None,
        incremental_mode: str = "since",
    ) -> tuple:
        """Run a full bulk export cycle: kick-off -> poll -> download -> cleanup.

        Returns (transaction_time, {resource_type: [resource_dicts]}).
        transaction_time is an ISO-8601 string suitable for use as a CDC cursor.
        For incremental_mode="full", transaction_time is None (caller should not
        advance the cursor).
        """
        # Captured before kick-off so it is never later than the export's own
        # snapshot point. Using the post-download clock would skip everything
        # written while the export ran.
        fallback_cursor = datetime.now(timezone.utc).isoformat()

        status_url = self._kick_off(resource_types, since, incremental_mode)
        try:
            manifest = self._poll_until_complete(status_url)
            resources = self._download_and_parse(manifest)
            if incremental_mode == "full":
                # Full mode re-exports all data every run — no server cursor.
                return None, resources
            transaction_time = manifest.get("transactionTime")
            if not transaction_time:
                logger.warning(
                    "Bulk export manifest missing transactionTime. Falling back "
                    "to the pre-kick-off UTC time as cursor — this may cause "
                    "duplicate records on the next incremental run."
                )
                transaction_time = fallback_cursor
            return transaction_time, resources
        finally:
            self._cleanup(status_url)

    def _kick_off(
        self,
        resource_types: list,
        since: Optional[str],
        incremental_mode: str,
    ) -> str:
        """Issue the async $export request. Returns the status polling URL."""
        url = self._export_url()
        params = {}

        if resource_types:
            # _type must always be sent. Per the Bulk Data IG, a Data Provider
            # "SHALL return all supported resources" when _type is omitted, and
            # _typeFilter only filters "resource types referenced in the
            # _typeFilter expression" -- so omitting _type makes the server
            # export every other type unfiltered.
            params["_type"] = ",".join(resource_types)

        if incremental_mode == "typefilter" and since and resource_types:
            # Epic does not support _since; express the date bound per type
            # instead. Sent alongside _type, matching the IG's own example.
            filters = [f"{rt}?_lastUpdated=gt{since}" for rt in resource_types]
            params["_typeFilter"] = ",".join(filters)
        elif since and incremental_mode == "since":
            params["_since"] = since

        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            # Re-fetch the token each attempt: the retry ladder spans up to
            # ~155s, which can outlive a cached token's remaining lifetime.
            headers = auth_headers(self._auth_client)
            headers["Prefer"] = "respond-async"
            resp = self._session.get(
                url,
                params=params,
                headers=headers,
                timeout=HTTP_TIMEOUT,
            )
            if resp.status_code == 202:
                status_url = resp.headers.get("Content-Location")
                if not status_url:
                    raise RuntimeError(
                        "Bulk export kick-off returned 202 but no Content-Location header. "
                        f"Response headers: {dict(resp.headers)}"
                    )
                logger.info("Bulk export started. Status URL: %s", status_url)
                return status_url
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                raise RuntimeError(
                    f"Bulk export kick-off failed (HTTP {resp.status_code}): {resp.text[:500]}"
                )
            if attempt < MAX_RETRIES - 1:
                sleep_seconds = _retry_after(resp, backoff, ceiling=MAX_RETRY_AFTER)
                logger.warning(
                    "Bulk export kick-off got %d, retrying in %.0fs (attempt %d/%d)",
                    resp.status_code,
                    sleep_seconds,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(sleep_seconds)
                backoff *= 2

        raise RuntimeError(
            f"Bulk export kick-off failed after {MAX_RETRIES} retries "
            f"(last HTTP {resp.status_code}): {resp.text[:500]}"
        )

    @staticmethod
    def _poll_sleep(resp: requests.Response, default: float, deadline: float) -> Optional[float]:
        """How long to wait before the next poll, or None if the budget is spent.

        The budget check keys off the remaining time, never off the requested
        sleep: a server may legitimately send `Retry-After: 0`, and treating
        that as "out of time" would abort an export that still has hours left.
        A short request is honoured but floored so the loop cannot busy-spin.
        """
        remaining = deadline - time.time()
        if remaining < BULK_MIN_POLL_SLEEP:
            return None
        requested = _retry_after(resp, default, ceiling=remaining)
        return max(requested, BULK_MIN_POLL_SLEEP)

    def _poll_until_complete(self, status_url: str) -> dict:
        """Poll the status URL until the export completes or times out.

        Retries on transient errors (429, 500, 502, 503) with backoff.
        """
        deadline = time.time() + self._timeout
        transient_backoff = INITIAL_BACKOFF
        while time.time() < deadline:
            resp = self._session.get(
                status_url,
                headers=auth_headers(self._auth_client),
                timeout=HTTP_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 202:
                progress = resp.headers.get("X-Progress", "")
                if progress:
                    logger.info("Bulk export progress: %s", progress)
                sleep_seconds = self._poll_sleep(resp, self._poll_interval, deadline)
                if sleep_seconds is None:
                    break  # budget spent
                transient_backoff = INITIAL_BACKOFF  # reset on successful poll
                time.sleep(sleep_seconds)
                continue

            if resp.status_code in RETRIABLE_STATUS_CODES:
                sleep_seconds = self._poll_sleep(resp, transient_backoff, deadline)
                if sleep_seconds is None:
                    break
                logger.warning(
                    "Bulk export poll got %d, retrying in %.0fs",
                    resp.status_code,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
                transient_backoff = min(transient_backoff * 2, 120)
                continue

            raise RuntimeError(
                f"Bulk export status check failed (HTTP {resp.status_code}): {resp.text[:500]}"
            )

        raise RuntimeError(
            f"Bulk export timed out after {self._timeout}s. Status URL: {status_url}"
        )

    def _download_and_parse(self, manifest: dict) -> dict:
        """Download NDJSON files from the manifest and group records by type."""
        # `is not False` rather than a get() default: an explicit JSON null must
        # keep auth on, not silently drop the bearer token from every download.
        requires_token = manifest.get("requiresAccessToken") is not False
        resources: dict = {}

        # `or []` rather than a get() default: a server may send an explicit
        # JSON null for these keys, and get(k, []) returns the stored None.
        for entry in manifest.get("output") or []:
            rtype = entry.get("type") or UNTYPED_BUCKET
            url = entry.get("url")
            if not url:
                continue
            records = list(self._download_ndjson(url, requires_token))
            resources.setdefault(rtype, []).extend(records)
            logger.info(
                "Downloaded %d %s resources from %s",
                len(records),
                rtype,
                url,
            )

        # The spec puts error, warning and information OperationOutcomes in the
        # same "error" array. Only error/fatal issues mean resources are missing
        # from the export; the caller would otherwise advance the cursor past
        # them and they would never be re-read. An outcome with no severity, or
        # an error file that cannot be read, is treated as an error: we cannot
        # tell what was lost.
        problems = []
        for entry in manifest.get("error") or []:
            url = entry.get("url")
            if not url:
                continue
            try:
                outcomes = list(self._download_ndjson(url, requires_token))
            except Exception as exc:
                problems.append(f"error file {url} could not be downloaded: {exc}")
                continue
            for outcome in outcomes:
                severities = {
                    (issue.get("severity") or "").lower() for issue in outcome.get("issue") or []
                }
                text = json.dumps(outcome)[:300]
                if not severities or severities - {"warning", "information"}:
                    problems.append(text)
                else:
                    logger.warning("Bulk export OperationOutcome: %s", text)

        if problems:
            if not self._ignore_errors:
                raise RuntimeError(
                    f"Bulk export reported {len(problems)} error(s), so the export is "
                    f"incomplete. The cursor is not advanced; the next trigger retries. "
                    f"Set bulk_export_ignore_errors=true to accept partial exports. "
                    f"First error: {problems[0]}"
                )
            for problem in problems:
                logger.warning("Bulk export error ignored (bulk_export_ignore_errors): %s", problem)

        # Deletions reported by a _since export (Bulk Data v2 "deleted" array) are
        # not applied: the tables are upsert-only outside full mode. Say so rather
        # than drop them silently.
        deleted = manifest.get("deleted") or []
        if deleted:
            logger.warning(
                "Bulk export manifest lists %d file(s) of deleted resources. Deletes are "
                "not applied in incremental bulk modes, so those rows stay in the target "
                "table. Use bulk_incremental_mode=full to propagate deletes.",
                len(deleted),
            )

        return resources

    def _download_ndjson(self, url: str, requires_access_token: bool = True):
        """Download and parse an NDJSON file, yielding one dict per line.

        Handles three download patterns:
        1. Direct HTTP + bearer token (Epic, HAPI, InterSystems, etc.)
        2. No auth / presigned URL (IBM, SMART test server)
        3. Redirect to presigned cloud storage (Cerner) — drops auth off-origin

        Redirects are followed manually so the Authorization header can be
        dropped when leaving the original host: presigned storage URLs carry
        their own credentials and reject (or, worse, log) a bearer token.
        Once dropped, auth is never re-attached, so a redirect chain that
        returns to the origin cannot be used to harvest the token.
        """
        # Scheme is part of the origin: an https -> http redirect on the same
        # host must not replay the bearer token over plaintext.
        _parsed = urlparse(url)
        origin = (_parsed.scheme.lower(), _parsed.netloc.lower())
        current = url
        send_auth = requires_access_token

        for _ in range(BULK_MAX_REDIRECTS + 1):
            headers = (
                auth_headers(self._auth_client, NDJSON) if send_auth else {"Accept": NDJSON}
            )
            resp = self._session.get(
                current,
                headers=headers,
                stream=True,
                allow_redirects=False,
                timeout=BULK_NDJSON_TIMEOUT,
            )
            if resp.status_code not in (301, 302, 303, 307, 308):
                break

            redirect_url = resp.headers.get("Location")
            resp.close()
            if not redirect_url:
                raise RuntimeError(f"NDJSON download got {resp.status_code} but no Location header")
            # Location may be relative (RFC 7231); resolve against the current URL.
            current = urljoin(current, redirect_url)
            nxt = urlparse(current)
            if (nxt.scheme.lower(), nxt.netloc.lower()) != origin:
                send_auth = False
        else:
            raise RuntimeError(f"NDJSON download exceeded {BULK_MAX_REDIRECTS} redirects for {url}")

        try:
            if resp.status_code != 200:
                raise RuntimeError(
                    f"NDJSON download failed (HTTP {resp.status_code}): {resp.text[:500]}"
                )
            for line in resp.iter_lines(decode_unicode=True):
                if line:
                    yield json.loads(line)
        finally:
            resp.close()

    def _cleanup(self, status_url: str) -> None:
        """Best-effort DELETE of the export job. Never raises."""
        try:
            self._session.delete(
                status_url,
                headers=auth_headers(self._auth_client),
                timeout=HTTP_TIMEOUT,
            )
        except Exception as exc:
            logger.warning("Bulk export cleanup failed for %s: %s", status_url, exc)
