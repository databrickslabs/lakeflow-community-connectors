"""Custom simulator handlers for the three Egnyte endpoints the declarative
param-role pipeline cannot express.

1. ``GET /pubapi/v1/fs/{path...}`` — a *composite* response. One call returns
   the folder's own envelope plus two sibling arrays (``folders`` and
   ``files``) drawn from two different corpora, and the connector recurses
   into ``folders[].path``. The declarative pipeline serves exactly one corpus
   per endpoint, so it can't produce both arrays, and without real child paths
   the tree walk would never recurse.

2. ``GET /pubapi/v2/users`` and ``GET /pubapi/v2/groups`` — SCIM paging uses a
   **1-based** ``startIndex``. Mapping it onto the simulator's 0-based
   ``offset`` role silently drops the first record; mapping it onto ``page``
   mis-multiplies once the connector advances by page length. A tiny handler
   is exact under any page size.

3. ``GET /pubapi/v2/groups/{id}`` — returns one group *with* its ``members``
   array. ``single_entity: true`` would hand back the whole groups corpus
   list, and there is no declarative "pick the record whose id matches the
   path param".
"""

from __future__ import annotations

import copy
import json
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_FS_PREFIX = "/pubapi/v1/fs/"
_DEFAULT_FS_COUNT = 100
_DEFAULT_SCIM_COUNT = 100


# ---------------------------------------------------------------------------
# File system
# ---------------------------------------------------------------------------


def fs_listing(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """Serve one folder listing of a two-level synthetic tree.

    Layout, for a request against root ``/<root>``:

      ``/<root>``                       the root folder, whose ``folders`` are
                                        the ``folders`` corpus (paths rebased
                                        under the root) and whose ``files`` are
                                        the ``files`` corpus (likewise).
      ``/<root>/<child>``               that corpus folder, with empty
                                        ``folders`` and ``files``.

    Keeping every file at the root means the ``files`` table sees each corpus
    record exactly once (no duplicate primary keys), while the two levels still
    exercise the connector's recursion, its visited-path guard, and its
    ``offset``/``count`` page merging.
    """
    path_part = _fs_request_path(prep.url or "")
    segments = [seg for seg in path_part.split("/") if seg]
    if not segments:
        return _json_response(
            prep,
            404,
            {"Errors": [{"description": "Folder does not exist.", "code": "404"}]},
        )

    root = segments[0]
    root_path = f"/{root}"

    child_folders = _rebase(corpus.get("folders") or [], root_path)
    child_files = _rebase(corpus.get("files") or [], root_path)

    if len(segments) == 1:
        envelope = _root_folder(root, root_path)
        items = [("folder", rec) for rec in child_folders]
        items += [("file", rec) for rec in child_files]
    else:
        wanted = "/" + "/".join(segments)
        match = next(
            (rec for rec in child_folders if rec.get("path") == wanted), None
        )
        envelope = match if match is not None else _leaf_folder(segments)
        envelope = copy.deepcopy(envelope)
        items = []

    query = _query(prep.url or "")
    offset = _int(query.get("offset"), 0)
    count = _int(query.get("count"), _DEFAULT_FS_COUNT)
    page = items[offset : offset + count] if count > 0 else []

    envelope = dict(envelope)
    envelope.pop("folders", None)
    envelope.pop("files", None)
    envelope.update(
        {
            "is_folder": True,
            "offset": offset,
            "count": len(page),
            "total_count": len(items),
            "folders": [rec for kind, rec in page if kind == "folder"],
            "files": [rec for kind, rec in page if kind == "file"],
        }
    )
    return _json_response(prep, 200, envelope)


def _fs_request_path(url: str) -> str:
    path = urlsplit(url).path
    if not path.startswith(_FS_PREFIX):
        return ""
    # The connector encodes each segment separately and leaves "/" literal,
    # so unquoting per segment round-trips correctly.
    return "/".join(unquote(seg) for seg in path[len(_FS_PREFIX) :].split("/"))


def _rebase(records: list, root_path: str) -> list[dict]:
    """Copy corpus records with their ``path`` rebased under ``root_path``.

    Corpus paths are synthesized identifiers (unique because ``path`` is part
    of the ``files`` primary key); prefixing preserves that uniqueness while
    producing paths that look like real Egnyte paths and resolve back to this
    handler on the recursive call.
    """
    out: list[dict] = []
    for raw in records:
        if not isinstance(raw, dict):
            continue
        record = copy.deepcopy(raw)
        leaf = str(record.get("path") or record.get("name") or "item").strip("/")
        leaf = leaf.rsplit("/", 1)[-1] or "item"
        record["path"] = f"{root_path}/{leaf}"
        record["name"] = record.get("name") or leaf
        out.append(record)
    return out


def _root_folder(root: str, root_path: str) -> dict:
    return {
        "name": root,
        "path": root_path,
        "folder_id": f"sim-root-{root}",
        "parent_id": None,
        "is_folder": True,
        "permission": "Owner",
        "folder_description": "",
        "public_links": "files_folders",
        "allow_links": True,
        "allow_upload_links": True,
        "restrict_move_delete": False,
    }


def _leaf_folder(segments: list[str]) -> dict:
    path = "/" + "/".join(segments)
    return {
        "name": segments[-1],
        "path": path,
        "folder_id": f"sim-folder-{'-'.join(segments)}",
        "parent_id": f"sim-root-{segments[0]}",
        "is_folder": True,
        "permission": "Editor",
        "folder_description": "",
        "public_links": "files_folders",
        "allow_links": True,
        "allow_upload_links": True,
        "restrict_move_delete": False,
    }


# ---------------------------------------------------------------------------
# SCIM-style user / group listing
# ---------------------------------------------------------------------------


def scim_list(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    """Page a corpus with a 1-based ``startIndex`` plus ``count``.

    Group list responses have ``members`` stripped: the real endpoint only
    returns it from the single-group GET, which is what makes the connector's
    ``include_members`` fan-out necessary.
    """
    records = corpus.get(spec.corpus) or []
    if not isinstance(records, list):
        records = []

    query = _query(prep.url or "")
    start_index = max(1, _int(query.get("startIndex"), 1))
    count = max(1, _int(query.get("count"), _DEFAULT_SCIM_COUNT))
    page = [copy.deepcopy(r) for r in records[start_index - 1 : start_index - 1 + count]]

    if spec.corpus == "groups":
        for record in page:
            record.pop("members", None)

    return _json_response(
        prep,
        200,
        {
            "schemas": ["urn:scim:schemas:core:1.0"],
            "totalResults": len(records),
            "itemsPerPage": len(page),
            "startIndex": start_index,
            "resources": page,
        },
    )


def group_detail(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    """Return one group, including the ``members`` array the list omits."""
    records = corpus.get(spec.corpus) or []
    group_id = unquote(urlsplit(prep.url or "").path.rsplit("/", 1)[-1])

    match = next(
        (r for r in records if isinstance(r, dict) and str(r.get("id")) == group_id),
        None,
    )
    if match is None:
        if not records:
            return _json_response(
                prep,
                404,
                {"Errors": [{"description": "Group does not exist.", "code": "404"}]},
            )
        match = records[0]

    payload = copy.deepcopy(match)
    payload["schemas"] = ["urn:scim:schemas:core:1.0"]
    payload.setdefault("members", [])
    return _json_response(prep, 200, payload)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _query(url: str) -> dict[str, str]:
    return dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))


def _int(raw: Any, default: int) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _json_response(prep: PreparedRequest, status: int, payload: Any) -> Response:
    body = json.dumps(payload, ensure_ascii=False)
    record = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json"},
        body_text=body,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(record, prep)
