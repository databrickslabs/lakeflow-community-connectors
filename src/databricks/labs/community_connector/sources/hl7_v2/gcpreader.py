"""Standalone GCP Healthcare API reader for HL7 v2 messages.

Run directly to fetch and inspect HL7 v2 messages from a Google Cloud
Healthcare API HL7v2 store.  Accepts parameters via command-line arguments
or a JSON config file (same format as dev_config_gcp.json).

Usage:
    # With a config file
    python gcpreader.py --config /path/to/dev_config_gcp.json

    # With individual arguments
    python gcpreader.py \
        --project-id my-project \
        --location us-central1 \
        --dataset-id my-dataset \
        --hl7v2-store-id my-store \
        --service-account-json /path/to/sa-key.json

    # Optional flags
    --limit 5               # number of messages to fetch (default 10)
    --since 2024-01-01      # only fetch messages after this timestamp
    --raw                   # print raw base64 data instead of decoded HL7
    --json-out              # print full API response as JSON
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
from pathlib import Path


def _field_sort_key(key: str) -> tuple:
    """Convert an HL7 field key into a sortable tuple of ints.

    '6.2'      → (6, 2)
    '21[0].1'  → (21, 0, 1)
    '0'        → (0,)
    """
    tokens = re.split(r"[\[\]\.]+", key)
    result: list[int | str] = []
    for t in tokens:
        if not t:
            continue
        try:
            result.append(int(t))
        except ValueError:
            result.append(t)
    return tuple(result)


def sort_fields(fields: dict) -> dict:
    """Return *fields* ordered by HL7 field number."""
    return dict(sorted(fields.items(), key=lambda kv: _field_sort_key(kv[0])))


def _load_service_account(value: str) -> dict:
    """Load service account info from a file path or inline JSON string."""
    path = Path(value)
    if path.is_file():
        return json.loads(path.read_text())
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        raise ValueError(
            f"service_account_json is neither a valid file path nor valid JSON: {value[:80]}"
        )


def _authenticate(sa_info: dict):
    """Create and refresh GCP credentials, return an authorized session."""
    from google.auth.transport.requests import Request
    from google.oauth2 import service_account
    import requests

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    creds.refresh(Request())
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {creds.token}"
    return session


def _build_url(project_id: str, location: str, dataset_id: str, hl7v2_store_id: str) -> str:
    return (
        f"https://healthcare.googleapis.com/v1"
        f"/projects/{project_id}"
        f"/locations/{location}"
        f"/datasets/{dataset_id}"
        f"/hl7V2Stores/{hl7v2_store_id}/messages"
    )


def fetch_messages(
    session,
    url: str,
    limit: int = 10,
    since: str | None = None,
) -> list[dict]:
    """Fetch up to *limit* messages from the HL7v2 store."""
    params: dict[str, str] = {
        "view": "FULL",
        "pageSize": str(min(limit, 1000)),
        "orderBy": "sendTime asc",
    }
    if since:
        params["filter"] = f'sendTime > "{since}"'

    all_messages: list[dict] = []
    page_token: str | None = None

    while len(all_messages) < limit:
        if page_token:
            params["pageToken"] = page_token
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()

        batch = body.get("hl7V2Messages", [])
        all_messages.extend(batch)

        page_token = body.get("nextPageToken")
        if not page_token:
            break

    return all_messages[:limit]


def decode_hl7(msg: dict) -> str:
    """Base64-decode the ``data`` field to raw HL7 pipe-delimited text."""
    raw = msg.get("data", "")
    if not raw:
        return ""
    return base64.b64decode(raw).decode("utf-8", errors="replace")


def _parse_field(fields: dict, idx: int, value: str) -> None:
    """Parse a single HL7 field value (handling repeats and components) into *fields*."""
    if not value:
        return
    repeats = value.split("~")
    has_repeats = len(repeats) > 1
    for r, repeat in enumerate(repeats):
        components = repeat.split("^")
        if len(components) == 1:
            key = f"{idx}[{r}]" if has_repeats else str(idx)
            fields[key] = components[0]
        else:
            for j, comp in enumerate(components, 1):
                if comp:
                    prefix = f"{idx}[{r}]" if has_repeats else str(idx)
                    fields[f"{prefix}.{j}"] = comp


def parse_segment(line: str) -> dict:
    """Parse a raw HL7 pipe-delimited segment into ``{segmentId, fields}``
    with fields sorted by field number."""
    seg_type = line[:3]
    fields: dict[str, str] = {"0": seg_type}

    if seg_type == "MSH":
        fields["1"] = line[3]
        rest = line[4:]
        parts = rest.split("|")
        fields["2"] = parts[0]
        for i, part in enumerate(parts[1:], 3):
            _parse_field(fields, i, part)
    else:
        rest = line[4:] if len(line) > 3 and line[3] == "|" else line[3:]
        parts = rest.split("|")
        for i, part in enumerate(parts, 1):
            _parse_field(fields, i, part)

    return {"segmentId": seg_type, "raw": line, "fields": sort_fields(fields)}


def print_message(idx: int, msg: dict, *, raw: bool = False) -> None:
    """Pretty-print a single HL7 API message."""
    print(f"\n{'=' * 60}")
    print(f"  Message {idx}")
    print(f"{'=' * 60}")
    print(f"  Resource name : {msg.get('name', 'N/A')}")
    print(f"  Send time     : {msg.get('sendTime', 'N/A')}")
    print(f"  Create time   : {msg.get('createTime', 'N/A')}")
    print(f"  Message type  : {msg.get('messageType', 'N/A')}")
    print(f"  Patient IDs   : {msg.get('patientIds', [])}")
    print(f"  Labels        : {msg.get('labels', {})}")
    print()

    if raw:
        print(f"  Raw data (base64): {msg.get('data', '')[:120]}...")
    else:
        hl7_text = decode_hl7(msg)
        if hl7_text:
            print("  Decoded HL7 segments:")
            for line in hl7_text.replace("\r\n", "\r").replace("\n", "\r").split("\r"):
                line = line.strip()
                if line:
                    seg_id = line[:3]
                    print(f"\n    [{seg_id}] {line}")
                    parsed = parse_segment(line)
                    for key, val in parsed["fields"].items():
                        print(f"      {seg_id}-{key:12s} = {val}")
        else:
            print("  (no data)")


def _resolve_params(args, cfg: dict) -> tuple[str, str, str, str, str]:
    """Resolve required GCP params from CLI args or the config file, in that order.

    Exits with a clear error message if any required parameter is missing.
    """
    project_id = args.project_id or cfg.get("project_id")
    location = args.location or cfg.get("location")
    dataset_id = args.dataset_id or cfg.get("dataset_id")
    hl7v2_store_id = args.hl7v2_store_id or cfg.get("hl7v2_store_id")
    sa_json = args.service_account_json or cfg.get("service_account_json")

    missing = [
        name for name, val in (
            ("project-id", project_id),
            ("location", location),
            ("dataset-id", dataset_id),
            ("hl7v2-store-id", hl7v2_store_id),
            ("service-account-json", sa_json),
        ) if not val
    ]
    if missing:
        print(f"Error: missing required parameters: {', '.join(missing)}", file=sys.stderr)
        print("Provide them via --config or individual flags.", file=sys.stderr)
        sys.exit(1)

    return project_id, location, dataset_id, hl7v2_store_id, sa_json


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Standalone reader for GCP Healthcare API HL7 v2 messages",
    )
    parser.add_argument("--config", help="Path to a JSON config file (dev_config_gcp.json format)")
    parser.add_argument("--project-id", help="GCP project ID")
    parser.add_argument("--location", help="GCP region (e.g. us-central1)")
    parser.add_argument("--dataset-id", help="Healthcare API dataset ID")
    parser.add_argument("--hl7v2-store-id", help="HL7v2 store name")
    parser.add_argument("--service-account-json", help="Path to SA key file or inline JSON")
    parser.add_argument("--limit", type=int, default=10, help="Max messages to fetch (default 10)")
    parser.add_argument("--since", help="Only fetch messages after this timestamp (RFC3339)")
    parser.add_argument("--raw", action="store_true", help="Print raw base64, not decoded HL7")
    parser.add_argument("--json-out", action="store_true", help="Print full API response as JSON")
    parser.add_argument(
        "--parsed-json", action="store_true",
        help="Print parsed segments as JSON with fields sorted by field number",
    )

    args = parser.parse_args()

    cfg: dict[str, str] = {}
    if args.config:
        cfg = json.loads(Path(args.config).read_text())

    project_id, location, dataset_id, hl7v2_store_id, sa_json = _resolve_params(args, cfg)
    sa_info = _load_service_account(sa_json)

    print("Authenticating with GCP...")
    session = _authenticate(sa_info)
    print("Authenticated successfully.\n")

    url = _build_url(project_id, location, dataset_id, hl7v2_store_id)
    print(f"API endpoint: {url}")
    print(f"Fetching up to {args.limit} messages...")
    if args.since:
        print(f"Filtering: sendTime > \"{args.since}\"")

    messages = fetch_messages(session, url, limit=args.limit, since=args.since)
    print(f"\nReceived {len(messages)} message(s).")

    if args.json_out:
        print(json.dumps(messages, indent=2))
        return

    if args.parsed_json:
        for i, msg in enumerate(messages, 1):
            hl7_text = decode_hl7(msg)
            if not hl7_text:
                continue
            segments = []
            for line in hl7_text.replace("\r\n", "\r").replace("\n", "\r").split("\r"):
                line = line.strip()
                if line:
                    segments.append(parse_segment(line))
            output = {
                "message": i,
                "resourceName": msg.get("name", "N/A"),
                "sendTime": msg.get("sendTime", "N/A"),
                "createTime": msg.get("createTime", "N/A"),
                "messageType": msg.get("messageType", "N/A"),
                "patientIds": msg.get("patientIds", []),
                "labels": msg.get("labels", {}),
                "segments": segments,
            }
            print(json.dumps(output, indent=2))
        return

    for i, msg in enumerate(messages, 1):
        print_message(i, msg, raw=args.raw)

    if not messages:
        print("\nNo messages found. Check your parameters or try removing the --since filter.")


if __name__ == "__main__":
    main()
