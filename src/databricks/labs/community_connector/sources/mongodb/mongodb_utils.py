"""Pure helpers for the MongoDB community connector.

No Spark, no pymongo client state — everything here is unit-testable in
isolation: option parsing, BSON -> JSON-safe conversion, and the offset model
the framework checkpoints.
"""

from __future__ import annotations

import base64
import hashlib
from datetime import date, datetime, timezone
from typing import Any

from bson import Binary, Decimal128, ObjectId
from bson.timestamp import Timestamp

SYSTEM_DBS = frozenset({"admin", "local", "config"})

# Bronze column names (kept here so connector + schema + tests agree on one source of truth).
COL_ID = "_id"
COL_OP = "_operation"
COL_CLUSTER_TIME = "_cluster_time"
COL_DB = "_db"
COL_COLLECTION = "_collection"
COL_DOCUMENT = "document"

OP_SNAPSHOT = "snapshot"
OP_INSERT = "insert"
OP_UPDATE = "update"
OP_REPLACE = "replace"
OP_DELETE = "delete"

VALID_INGESTION_TYPES = frozenset({"snapshot", "cdc", "cdc_with_deletes", "append"})


def system_db(name: str) -> bool:
    """True for MongoDB internal databases that should never be ingested."""
    return name in SYSTEM_DBS


def redact(uri: str) -> str:
    """Mask credentials in a connection URI for safe logging."""
    if "@" not in uri or "//" not in uri:
        return uri
    scheme, rest = uri.split("//", 1)
    creds, host = rest.split("@", 1)
    return f"{scheme}//***:***@{host}"


def parse_options(options: dict[str, str]) -> dict[str, Any]:
    """Extract connector configuration from the framework's merged options dict.

    The framework injects UC-connection options plus per-table options. Only the
    keys we care about are pulled out; unknown keys are ignored.
    """
    uri = options.get("connection_uri") or options.get("connectionUri")
    if not uri:
        raise ValueError("MongoDB connector requires a 'connection_uri' option")
    allow = options.get("database_allowlist", "")
    allowlist = frozenset(d.strip() for d in allow.split(",") if d.strip())
    return {
        "connection_uri": uri,
        "database": options.get("database"),
        "database_allowlist": allowlist,
    }


def client_options(options: dict[str, str]) -> dict:
    """Build production-hardened ``MongoClient`` keyword options from connector options.

    Defaults read from **secondaries** (``secondaryPreferred``) so ingestion never
    loads the primary the application writes to — the Fivetran-style protection of
    the source — and enable read retries + a workload-tagged appName. All values
    are JSON/primitive types so the connector stays picklable.
    """
    rp = options.get("read_preference", "secondaryPreferred")
    opts: dict = {
        "readPreference": rp,
        "retryReads": options.get("retry_reads", "true").lower() != "false",
        "serverSelectionTimeoutMS": int(options.get("server_selection_timeout_ms", "30000")),
        "connectTimeoutMS": int(options.get("connect_timeout_ms", "20000")),
        # FINITE socket timeout — pymongo's default is infinite, which can wedge a
        # Spark executor slot forever on a half-open connection.
        "socketTimeoutMS": int(options.get("socket_timeout_ms", "120000")),
        "heartbeatFrequencyMS": int(options.get("heartbeat_frequency_ms", "5000")),
        # Bound the per-executor pool so maxPoolSize x executors x members stays
        # under the Atlas tier connection limit.
        "maxPoolSize": int(options.get("max_pool_size", "20")),
        "maxIdleTimeMS": int(options.get("max_idle_time_ms", "300000")),
        "appName": options.get("app_name", "databricks-lakeflow-mongodb"),
    }
    # maxStalenessSeconds is only valid for non-primary read preferences (min 90s).
    if rp != "primary" and options.get("max_staleness_seconds"):
        opts["maxStalenessSeconds"] = int(options["max_staleness_seconds"])
    if options.get("read_concern_level"):
        opts["readConcernLevel"] = options["read_concern_level"]
    if options.get("compressors"):
        opts["compressors"] = options["compressors"]
    if options.get("tls"):
        opts["tls"] = options["tls"].lower() == "true"
    if options.get("tls_ca_file"):
        opts["tlsCAFile"] = options["tls_ca_file"]
    return opts


def resolve_db_collection(
    table_name: str,
    table_options: dict[str, str],
    default_database: str | None,
) -> tuple[str, str]:
    """Resolve (database, collection) for a table across the framework's conventions.

    Precedence: explicit ``database`` option > ``namespace`` option (JSON list) >
    dotted ``db.collection`` table name > connector default database. Dotted
    table names are supported for convenience but a ``database`` option is the
    recommended, collision-free form.
    """
    db_opt = table_options.get("database")
    if db_opt:
        return db_opt, table_name
    ns = table_options.get("namespace")
    if ns:
        import json

        parts = json.loads(ns) if ns.startswith("[") else [ns]
        if parts:
            return parts[0], table_name
    if "." in table_name:
        db, coll = table_name.split(".", 1)
        return db, coll
    if default_database:
        return default_database, table_name
    raise ValueError(
        f"Cannot resolve database for table '{table_name}'. Provide a 'database' "
        "option on the connection or table, or use a 'db.collection' table name."
    )


# --------------------------------------------------------------------------- #
# BSON -> JSON-safe conversion (salvaged + hardened from PR #56)
# --------------------------------------------------------------------------- #


def convert_value(value: Any) -> Any:
    """Convert a single BSON value into a JSON-serialisable Python value.

    Lossless where it matters: ObjectId/Decimal128 -> string (no float rounding),
    datetime -> ISO-8601 UTC with trailing ``Z``, Binary -> base64 string.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Decimal128):
        return str(value.to_decimal())
    if isinstance(value, (datetime, date)):
        dt = value
        if isinstance(dt, datetime) and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    if isinstance(value, Timestamp):
        return {"t": value.time, "i": value.inc}
    if isinstance(value, (bytes, Binary)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, list):
        return [convert_value(v) for v in value]
    if isinstance(value, dict):
        return {k: convert_value(v) for k, v in value.items()}
    return str(value)  # last-resort: BSON types we don't special-case (Code, Regex, ...)


def convert_document(doc: dict) -> dict:
    """Convert a full BSON document into a JSON-serialisable dict."""
    return {k: convert_value(v) for k, v in doc.items()}


# --------------------------------------------------------------------------- #
# Connector-side PII handling (drop / hash named fields BEFORE they land)
# --------------------------------------------------------------------------- #


def parse_pii(options: dict[str, str]) -> dict | None:
    """Build PII config from options, or None if no PII handling is requested.

    ``pii_drop_fields`` / ``pii_hash_fields`` are comma-separated dotted paths
    (e.g. ``card.pan, member.ssn``). Dropped fields never land; hashed fields are
    replaced with ``sha256(salt + value)`` so they land pseudonymized.
    """
    drop = frozenset(p.strip() for p in options.get("pii_drop_fields", "").split(",") if p.strip())
    hashed = frozenset(
        p.strip() for p in options.get("pii_hash_fields", "").split(",") if p.strip()
    )
    if not drop and not hashed:
        return None
    return {"drop": drop, "hash": hashed, "salt": options.get("pii_hash_salt", "")}


def _walk_to_parent(obj: Any, parts: list[str]):
    """Return the parent dict holding the leaf key, or None if the path is absent."""
    for p in parts[:-1]:
        if not isinstance(obj, dict) or p not in obj:
            return None
        obj = obj[p]
    return obj if isinstance(obj, dict) else None


def apply_pii(doc: dict, pii: dict | None) -> dict:
    """Drop and/or hash configured fields on a JSON-safe document, in place."""
    if not pii:
        return doc
    for path in pii["drop"]:
        parent = _walk_to_parent(doc, path.split("."))
        if parent is not None:
            parent.pop(path.split(".")[-1], None)
    for path in pii["hash"]:
        parent = _walk_to_parent(doc, path.split("."))
        leaf = path.split(".")[-1]
        if parent is not None and parent.get(leaf) is not None:
            digest = hashlib.sha256((pii["salt"] + str(parent[leaf])).encode()).hexdigest()
            parent[leaf] = digest
    return doc


# --------------------------------------------------------------------------- #
# _id codec (handles ObjectId and non-ObjectId keys) + bronze row builders
# --------------------------------------------------------------------------- #


def encode_id(_id: Any) -> dict:
    """Encode a document ``_id`` into a JSON-safe, type-preserving offset value."""
    if isinstance(_id, ObjectId):
        return {"t": "oid", "v": str(_id)}
    return {"t": "raw", "v": convert_value(_id)}


def decode_id(enc: dict | None) -> Any:
    """Reconstruct a queryable ``_id`` value from :func:`encode_id` output."""
    if not enc:
        return None
    return ObjectId(enc["v"]) if enc.get("t") == "oid" else enc["v"]


def _doc_row(
    op: str,
    _id: Any,
    db: str,
    coll: str,
    document_obj: Any,
    cluster_time: int | None,
    pii: dict | None = None,
) -> dict:
    import json

    if document_obj is not None and pii:
        document_obj = apply_pii(document_obj, pii)
    return {
        COL_ID: str(_id),
        COL_OP: op,
        COL_CLUSTER_TIME: cluster_time,
        COL_DB: db,
        COL_COLLECTION: coll,
        COL_DOCUMENT: json.dumps(document_obj) if document_obj is not None else None,
    }


def snapshot_row(
    doc: dict, db: str, coll: str, cluster_time: int | None = None, pii: dict | None = None
) -> dict:
    return _doc_row(OP_SNAPSHOT, doc.get("_id"), db, coll, convert_document(doc), cluster_time, pii)


def append_row(
    doc: dict, db: str, coll: str, cluster_time: int | None = None, pii: dict | None = None
) -> dict:
    return _doc_row(OP_INSERT, doc.get("_id"), db, coll, convert_document(doc), cluster_time, pii)


def change_row(change: dict, db: str, coll: str, pii: dict | None = None) -> dict:
    """Map an insert/update/replace/delete change-stream event to a bronze row."""
    full = change.get("fullDocument")
    if full is None:  # delete events (soft-delete mode) carry only the pre-image
        full = change.get("fullDocumentBeforeChange")
    _id = (change.get("documentKey") or {}).get("_id")
    document_obj = convert_document(full) if full is not None else None
    return _doc_row(
        change.get("operationType"),
        _id,
        db,
        coll,
        document_obj,
        bson_ts_to_long(change.get("clusterTime")),
        pii,
    )


def delete_row(change: dict, db: str, coll: str, pii: dict | None = None) -> dict:
    """Map a delete change-stream event to a tombstone row (pre-image if available)."""
    _id = (change.get("documentKey") or {}).get("_id")
    before = change.get("fullDocumentBeforeChange")
    document_obj = convert_document(before) if before is not None else None
    return _doc_row(
        OP_DELETE, _id, db, coll, document_obj, bson_ts_to_long(change.get("clusterTime")), pii
    )


# --------------------------------------------------------------------------- #
# Offset model — opaque JSON-safe dicts the framework checkpoints
# --------------------------------------------------------------------------- #


def bson_ts_to_long(ts: Timestamp | None) -> int | None:
    """Encode a BSON cluster Timestamp as a sortable long (seconds<<32 | inc)."""
    if ts is None:
        return None
    return (int(ts.time) << 32) | int(ts.inc)


def append_offset(last_id: str | None) -> dict:
    """Offset for append/incremental tables — cursor on the last ``_id`` seen."""
    return {"phase": "append", "last_id": last_id}


def stream_offset(resume_token: dict | None, cluster_time: int | None) -> dict:
    """Offset for cdc tables — change-stream resume token + cluster time fallback."""
    return {"phase": "stream", "resume_token": resume_token, "cluster_time": cluster_time}


def is_same_offset(a: dict | None, b: dict | None) -> bool:
    """Framework convergence is equality-only; normalise None/{} as equivalent."""
    return (a or {}) == (b or {})
