"""Audit Reporting v1 — the job-based create → poll → fetch lifecycle.

Kept out of the connector module because it is a small state machine rather
than a read: a report does not exist until you ask for one, the answer arrives
asynchronously, and only then is there anything to paginate.

    POST /pubapi/v1/audit/{type}      → 202 {"id": ...}
    GET  /pubapi/v1/audit/jobs/{id}   → 200 {"status": "running"} | 303 + Location
    GET  /pubapi/v1/audit/{type}/{id} → 200 {"total_count", "offset", "count", "events"}

Everything here is a module-level function taking an ``EgnyteClient``, so one
window runs self-contained on a Spark executor with no driver state.

Requires an admin, or a power user with "can run reports" — a standard-user
token gets ``403`` on these endpoints even though it works fine for the File
System, Users, and Groups APIs.

Audit Reporting **v2** (``/pubapi/v2/audit/stream``) is deliberately not used:
it only serves the trailing 7 days and carries its own much tighter 10/min,
100/hour budget, so it cannot backfill. It is a future low-latency add-on, not
a replacement for v1.
"""

from __future__ import annotations

import time
from typing import Any, Iterator

from databricks.labs.community_connector.sources.egnyte.egnyte_client import (
    EgnyteApiError,
    EgnyteClient,
)
from databricks.labs.community_connector.sources.egnyte.egnyte_schemas import (
    AUDIT_RUNNING_STATUSES,
    DEFAULT_AUDIT_LOGIN_EVENTS,
    DEFAULT_AUDIT_PAGE_SIZE,
    DEFAULT_AUDIT_PERMISSION_FOLDERS,
    DEFAULT_AUDIT_POLL_INTERVAL_SECONDS,
    DEFAULT_AUDIT_POLL_MAX_ATTEMPTS,
    MAX_PAGES_PER_READ,
)
from databricks.labs.community_connector.sources.egnyte.egnyte_utils import (
    csv_option,
    parse_int,
    parse_optional_int,
)


def read_audit_window(
    client: EgnyteClient,
    domain: str,
    audit_type: str,
    date_start: str,
    date_end: str,
    table_options: dict[str, str],
) -> Iterator[dict]:
    """Run one full report lifecycle for an inclusive ``date_start..date_end``."""
    job_id = create_job(client, audit_type, date_start, date_end, table_options)
    await_job(client, job_id, table_options)
    yield from fetch_report(client, domain, audit_type, job_id, table_options)


def create_job(
    client: EgnyteClient,
    audit_type: str,
    date_start: str,
    date_end: str,
    table_options: dict[str, str],
) -> str:
    """Submit a report job and return its id.

    Each report type takes a different filter set. ``permissions`` is the
    fussy one: the SDK signature marks all four of ``folders``,
    ``assigners``, ``assignee_users`` and ``assignee_groups`` as required, so
    they are always sent (empty lists mean "no restriction").
    """
    body: dict[str, Any] = {
        "format": "json",
        "date_start": date_start,
        "date_end": date_end,
    }

    if audit_type == "logins":
        body["events"] = csv_option(
            table_options.get("audit_events"), DEFAULT_AUDIT_LOGIN_EVENTS
        )
    elif audit_type == "files":
        # All optional on this report type; only send what was configured.
        for option_key, field in (
            ("audit_folders", "folders"),
            ("audit_users", "users"),
            ("audit_transaction_types", "transaction_type"),
        ):
            values = csv_option(table_options.get(option_key), ())
            if values:
                body[field] = values
        if table_options.get("audit_file"):
            body["file"] = table_options["audit_file"]
    else:  # permissions
        body["folders"] = csv_option(
            table_options.get("audit_folders"), DEFAULT_AUDIT_PERMISSION_FOLDERS
        )
        body["assigners"] = csv_option(table_options.get("audit_assigners"), ())
        body["assignee_users"] = csv_option(
            table_options.get("audit_assignee_users"), ()
        )
        body["assignee_groups"] = csv_option(
            table_options.get("audit_assignee_groups"), ()
        )

    endpoint = f"/pubapi/v1/audit/{audit_type}"
    response = client.post_json(endpoint, json_body=body)
    job_id = response.get("id") or response.get("job_id")
    if not job_id:
        raise EgnyteApiError(
            202,
            f"audit {audit_type} report job for {date_start}..{date_end} "
            f"returned no job id (body keys: {sorted(response)})",
            client.url(endpoint),
        )
    return str(job_id)


def await_job(
    client: EgnyteClient, job_id: str, table_options: dict[str, str]
) -> None:
    """Poll until the report is ready, or give up after a bounded number of tries.

    Two completion signals are accepted. The documented one is ``303 See
    Other`` with a ``Location`` header — which is why redirects are not
    followed here, otherwise requests would resolve it away before we could
    see it. Some deployments instead answer ``200`` with a terminal
    ``status``; anything that is not explicitly a *running* status is treated
    as done.

    Official guidance is to poll no more than once every two minutes, which is
    the ``audit_poll_interval_seconds`` default.
    """
    interval = parse_int(
        table_options.get("audit_poll_interval_seconds"),
        DEFAULT_AUDIT_POLL_INTERVAL_SECONDS,
        minimum=0,
    )
    max_attempts = parse_int(
        table_options.get("audit_poll_max_attempts"),
        DEFAULT_AUDIT_POLL_MAX_ATTEMPTS,
        minimum=1,
    )
    endpoint = f"/pubapi/v1/audit/jobs/{job_id}"

    for attempt in range(max_attempts):
        response = client.get_raw(
            endpoint, expected=(200, 303), allow_redirects=False
        )
        if response.status_code == 303:
            return

        payload = response.json() if (response.content or b"").strip() else {}
        status = str(payload.get("status", "")).strip().lower()
        if status not in AUDIT_RUNNING_STATUSES:
            return

        if attempt < max_attempts - 1 and interval:
            time.sleep(interval)

    raise EgnyteApiError(
        504,
        f"audit report job {job_id} did not complete after {max_attempts} polls",
        client.url(endpoint),
    )


def fetch_report(
    client: EgnyteClient,
    domain: str,
    audit_type: str,
    job_id: str,
    table_options: dict[str, str],
) -> Iterator[dict]:
    """Page through a completed report's rows."""
    page_size = parse_int(
        table_options.get("audit_page_size"), DEFAULT_AUDIT_PAGE_SIZE, minimum=1
    )
    endpoint = f"/pubapi/v1/audit/{audit_type}/{job_id}"
    offset = 0

    for _ in range(MAX_PAGES_PER_READ):
        body = client.get_json(
            endpoint, params={"offset": offset, "count": page_size}
        )
        batch = body.get("events") or []
        if not batch:
            break

        for raw in batch:
            record = dict(raw)
            record["egnyte_domain"] = domain
            yield record

        offset += len(batch)
        total = parse_optional_int(body.get("total_count"))
        if total is not None and offset >= total:
            break
        if len(batch) < page_size:
            break
