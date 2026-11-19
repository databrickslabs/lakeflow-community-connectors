"""In-memory stand-ins for the Pipedrive HTTP client, shared by the unit tests.

They are needed because there is no Pipedrive account.  They honour the query
parameters the connector relies on (``updated_since`` / ``updated_until``,
``status``, ``sort``, ``limit`` and pagination), so the connector's own read logic
runs against them.  The simulator suite covers the wire format.
"""

from databricks.labs.community_connector.sources.pipedrive.pipedrive_utils import (
    normalize_ts,
)


def timestamp_key(value):
    return normalize_ts(value)


class V2Fake:
    """Stand-in for ``PipedriveClient.iter_v2_pages`` over v2 lists.

    ``ignore_status=True`` models a server that ignores the ``status`` filter.
    ``on_request(fake, path)`` runs before each list request, so tests can mutate
    the data mid-walk.
    """

    def __init__(self, tables, *, ignore_status=False, on_request=None):
        self.tables = tables
        self.ignore_status = ignore_status
        self.on_request = on_request
        self.requests = []

    def iter_v2_pages(self, path, params):
        self.requests.append((path, dict(params)))
        if self.on_request:
            self.on_request(self, path)
        rows = [dict(r) for r in self.tables.get(path, [])]
        if params.get("updated_since"):
            rows = [r for r in rows if timestamp_key(r["update_time"]) >= params["updated_since"]]
        if params.get("updated_until"):
            rows = [r for r in rows if timestamp_key(r["update_time"]) <= params["updated_until"]]
        if params.get("status") and not self.ignore_status:
            allowed = set(params["status"].split(","))
            rows = [r for r in rows if r.get("status") in allowed]
        rows.sort(key=lambda r: (timestamp_key(r["update_time"]), r["id"]))
        limit = int(params["limit"])
        for i in range(0, len(rows), limit):
            yield rows[i : i + limit]


class V1Fake:
    """Stand-in for ``PipedriveClient.get_json`` / ``iter_v1_pages`` over v1 lists.

    ``ignore_since`` names paths whose server ignores ``updated_since`` (seen on
    ``/v1/leads/archived``).  ``sort`` honours ``update_time``, ``add_time`` and
    ``id``, with the id as tiebreak, like the real endpoints.
    """

    def __init__(self, lists, *, ignore_since=(), on_request=None):
        self.lists = lists
        self.ignore_since = set(ignore_since)
        self.on_request = on_request
        self.requests = []

    def get_json(self, path, params):
        self.requests.append((path, dict(params)))
        if self.on_request:
            self.on_request(self, path, params)
        rows = [dict(r) for r in self.lists.get(path, [])]
        since = params.get("updated_since")
        if since and path not in self.ignore_since:
            rows = [r for r in rows if timestamp_key(r["update_time"]) >= since]
        field = (params.get("sort") or "id ASC").split(",")[0].split()[0]
        if field == "update_time":
            rows.sort(key=lambda r: (timestamp_key(r["update_time"]), r["id"]))
        elif field == "add_time":
            rows.sort(key=lambda r: (timestamp_key(r["add_time"]), r["id"]))
        else:
            rows.sort(key=lambda r: r["id"])
        start = int(params.get("start", 0))
        limit = int(params["limit"])
        page = rows[start : start + limit]
        more = start + len(page) < len(rows)
        pagination = {"start": start, "limit": limit, "more_items_in_collection": more}
        if more:
            pagination["next_start"] = start + len(page)
        return {"success": True, "data": page, "additional_data": {"pagination": pagination}}

    def iter_v1_pages(self, path, params):
        query = dict(params)
        while True:
            body = self.get_json(path, query)
            data = body.get("data") or []
            yield data
            pagination = body["additional_data"]["pagination"]
            if not pagination["more_items_in_collection"]:
                return
            query["start"] = pagination["next_start"]


def drain(read, offset, max_calls=500):
    """Call ``read(offset)`` until the offset stops changing; return (rows, offset).

    Asserts the framework contract: a batch that returns rows with an unchanged
    offset would be dropped by Spark, so it is a failure here.
    """
    offset = offset or {}
    rows = []
    for _ in range(max_calls):
        records, end = read(offset)
        batch = list(records)
        if end == offset:
            assert not batch, f"rows {[r.get('id') for r in batch]} returned with an unchanged offset"
            return rows, end
        rows.extend(batch)
        offset = end
    raise AssertionError("read did not converge")
