"""Synthetic Rootly JSON:API pages, including opaque incident cursors."""

import json
from datetime import datetime
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

from requests import Response


def list_resources(prep, spec, corpus):
    """Serve pagination and incident time windows over synthetic records."""
    parts = urlsplit(prep.url)
    query = {key: values[0] for key, values in parse_qs(parts.query).items()}
    allowed = {"page[number]", "page[size]"}
    if spec.corpus == "incidents":
        allowed.update({"page[after]", "filter[updated_at][gte]", "filter[updated_at][lt]", "sort"})
    if set(query) - allowed:
        raise ValueError("Unexpected Rootly query parameter in simulator")
    size = int(query.get("page[size]", "100"))
    if "page[after]" in query:
        cursor = query["page[after]"]
        if not cursor.startswith("synthetic-"):
            raise ValueError("Invalid synthetic cursor")
        offset = int(cursor.removeprefix("synthetic-"))
    else:
        offset = (int(query.get("page[number]", "1")) - 1) * size
    records = corpus.get(spec.corpus)
    if spec.corpus == "incidents":

        def timestamp(value):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))

        since = query.get("filter[updated_at][gte]")
        until = query.get("filter[updated_at][lt]")
        records = [
            row
            for row in records
            if (since is None or timestamp(row["attributes"]["updated_at"]) >= timestamp(since))
            and (until is None or timestamp(row["attributes"]["updated_at"]) < timestamp(until))
        ]
        if "sort" in query:
            if query["sort"] != "updated_at":
                raise ValueError("Unsupported synthetic Rootly sort")
            records = sorted(
                records, key=lambda row: (timestamp(row["attributes"]["updated_at"]), row["id"])
            )
    page = records[offset : offset + size]
    more = offset + size < len(records)
    current_page = offset // size + 1
    meta = {
        "current_page": current_page,
        "next_page": current_page + 1 if more else None,
        "prev_page": current_page - 1 if current_page > 1 else None,
        "total_count": len(records),
        "total_pages": (len(records) + size - 1) // size,
    }
    if spec.corpus == "incidents":
        meta["next_cursor"] = f"synthetic-{offset + size}" if more else None
        if "page[number]" not in query:
            meta.update(current_page=None, next_page=None, prev_page=None)

    def link(number):
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                urlencode(
                    {
                        "page[number]": number,
                        "page[size]": size,
                    }
                ),
                "",
            )
        )

    body = {
        "data": page,
        "meta": meta,
        "links": {
            "self": prep.url,
            "first": link(1),
            "prev": link(current_page - 1) if current_page > 1 else None,
            "next": link(current_page + 1) if more else None,
            "last": link(max(1, meta["total_pages"])),
        },
    }
    response = Response()
    response.status_code = 200
    response.headers["Content-Type"] = "application/vnd.api+json"
    response.encoding = "utf-8"
    response._content = json.dumps(body).encode("utf-8")  # pylint: disable=protected-access
    response.request = prep
    response.url = prep.url
    return response
