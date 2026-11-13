"""A small in-process stand-in for the parts of a MongoDB replica set the connector uses.

The HTTP source simulator cannot stand in for MongoDB's wire protocol, and
``mongomock`` has no change streams. This fake implements just enough of the
PyMongo surface -- ``find`` with the connector's ``$expr`` paging, change
streams with resume tokens and post-batch resume tokens, ``listCollections`` --
to run the connector's logic offline in CI. It is a test double, not a
MongoDB: behaviour that depends on the real server (oplog loss, invalid
UTF-8 on the wire, sharding) is covered by ``test_mongodb_live.py`` against a
real replica set.
"""

from __future__ import annotations

import copy
import itertools
import threading
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from bson import Binary, Decimal128, Int64, ObjectId
from bson.timestamp import Timestamp

# BSON comparison order (https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/)
_ORDER = {type(None): 1, int: 2, Int64: 2, float: 2, Decimal128: 2, str: 3, dict: 4, list: 5,
          Binary: 6, bytes: 6, ObjectId: 7, bool: 8, datetime: 9, Timestamp: 10}


def _key(value: Any):
    rank = _ORDER.get(type(value), 11)
    if rank == 2:
        return (rank, Decimal(str(value.to_decimal() if isinstance(value, Decimal128) else value)))
    if rank == 4:
        return (rank, tuple((k, _key(v)) for k, v in value.items()))
    if rank == 5:
        return (rank, tuple(_key(v) for v in value))
    if rank == 7:
        return (rank, value.binary)
    return (rank, value)


def _match(doc: dict, query: dict) -> bool:
    if not query:
        return True
    if set(query) == {"_id"}:
        return _key(doc["_id"]) == _key(query["_id"])
    expr = query.get("$expr")
    if expr is None:
        raise NotImplementedError(f"fake_mongo does not support query {query!r}")
    clauses = expr["$and"] if "$and" in expr else [expr]
    for clause in clauses:
        (op, (field, lit)), = clause.items()
        assert field == "$_id"
        left, right = _key(doc["_id"]), _key(lit["$literal"])
        if (op == "$gt" and not left > right) or (op == "$lte" and not left <= right):
            return False
    return True


class _Cursor:
    def __init__(self, docs: list[dict]):
        self._docs = docs
        self._limit = 0

    def sort(self, field: str, direction: int):
        assert field == "_id" and direction == 1
        self._docs.sort(key=lambda d: _key(d["_id"]))
        return self

    def limit(self, n: int):
        self._limit = n
        return self

    def batch_size(self, _n: int):
        return self

    def __iter__(self):
        docs = self._docs[: self._limit] if self._limit else self._docs
        return iter(copy.deepcopy(docs))


class _Server:
    """Shared oplog and data of the fake deployment."""

    def __init__(self):
        self.lock = threading.Lock()
        self.data: dict[tuple[str, str], dict] = {}
        self.types: dict[tuple[str, str], str] = {}
        self.oplog: list[dict] = []
        self.read_concerns: list = []
        self.read_preferences: list = []
        self._inc = itertools.count(1)

    def token(self, ts: Timestamp, seq: int, high_water: bool = False) -> str:
        # Hex, ordered like real resume tokens: time, then increment.
        return f"82{ts.time:08X}{ts.inc:08X}{seq:08X}{'00' if high_water else '01'}"

    def now(self) -> Timestamp:
        return Timestamp(int(time.time()), next(self._inc))

    def log(self, ns: tuple[str, str], op: str, doc_id: Any, full: Optional[dict]):
        ts = self.now()
        seq = len(self.oplog) + 1
        self.oplog.append({"ns": ns, "op": op, "id": doc_id, "full": copy.deepcopy(full), "ts": ts,
                           "token": self.token(ts, seq)})


class FakeCollection:
    def __init__(self, server: _Server, db: str, name: str):
        self._s, self._ns = server, (db, name)
        self.read_concern = None

    @property
    def _docs(self) -> dict:
        return self._s.data.setdefault(self._ns, {})

    # -- writes (used by tests to produce changes) --
    def insert_many(self, docs: list[dict]):
        for d in docs:
            self.insert_one(d)

    def insert_one(self, doc: dict):
        with self._s.lock:
            self._s.types.setdefault(self._ns, "collection")
            doc = dict(doc)
            doc.setdefault("_id", ObjectId())
            key = _key(doc["_id"])
            if key in self._docs:
                raise ValueError(f"duplicate _id {doc['_id']!r}")
            self._docs[key] = doc
            self._s.log(self._ns, "insert", doc["_id"], doc)

    def update_one(self, flt: dict, update: dict):
        with self._s.lock:
            for doc in list(self._docs.values()):
                if _match(doc, flt):
                    doc.update(update["$set"])
                    self._s.log(self._ns, "update", doc["_id"], None)
                    return

    def replace_one(self, flt: dict, replacement: dict):
        with self._s.lock:
            for key, doc in list(self._docs.items()):
                if _match(doc, flt):
                    self._docs[key] = {"_id": doc["_id"], **replacement}
                    self._s.log(self._ns, "replace", doc["_id"], self._docs[key])
                    return

    def delete_one(self, flt: dict):
        with self._s.lock:
            for key, doc in list(self._docs.items()):
                if _match(doc, flt):
                    del self._docs[key]
                    self._s.log(self._ns, "delete", doc["_id"], None)
                    return

    def drop(self):
        with self._s.lock:
            self._s.data.pop(self._ns, None)
            self._s.types.pop(self._ns, None)
            self._s.log(self._ns, "drop", None, None)

    # -- reads (used by the connector) --
    def find(self, query: dict):
        self._s.read_concerns.append(getattr(self.read_concern, "level", None))
        with self._s.lock:
            return _Cursor([d for d in self._docs.values() if _match(d, query)])

    def find_one(self, query: dict):
        with self._s.lock:
            for d in self._docs.values():
                if _match(d, query):
                    return copy.deepcopy(d)
        return None

    def watch(self, pipeline=None, resume_after=None, start_at_operation_time=None,
              full_document=None, max_await_time_ms=None, batch_size=None):
        del max_await_time_ms, batch_size
        return _ChangeStream(self._s, self._ns, pipeline or [], resume_after,
                             start_at_operation_time, full_document)


class _ChangeStream:
    def __init__(self, server: _Server, ns, pipeline, resume_after, start_at, full_document):
        self._s, self._ns, self._full = server, ns, full_document
        match = pipeline[0]["$match"] if pipeline else {}
        self._ops = set(match.get("operationType", {}).get("$in", [])) if "operationType" in match \
            else (None if not match else set())
        with server.lock:
            if resume_after:
                after = resume_after["_data"]
                self._pos = sum(1 for e in server.oplog if e["token"] <= after)
            elif start_at is not None:
                self._pos = sum(1 for e in server.oplog if e["ts"] < start_at)
            else:
                self._pos = len(server.oplog)
            self.resume_token = resume_after or {"_data": self._hw_token()}
        self.alive = True

    def _hw_token(self) -> str:
        if self._pos == 0:
            return self._s.token(Timestamp(0, 0), 0, high_water=True)
        last = self._s.oplog[self._pos - 1]
        return last["token"][:-2] + "02"  # just after the last scanned entry

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.alive = False
        return False

    def try_next(self):
        with self._s.lock:
            while self._pos < len(self._s.oplog):
                entry = self._s.oplog[self._pos]
                self._pos += 1
                if entry["ns"] != self._ns:
                    continue
                op = entry["op"]
                if self._ops is not None and op not in self._ops:
                    continue
                event = {"_id": {"_data": entry["token"]}, "operationType": op,
                         "clusterTime": entry["ts"], "ns": {"db": self._ns[0], "coll": self._ns[1]}}
                if entry["id"] is not None:
                    event["documentKey"] = {"_id": entry["id"]}
                if op in ("insert", "replace"):
                    event["fullDocument"] = copy.deepcopy(entry["full"])
                elif op == "update" and self._full == "updateLookup":
                    current = self._s.data.get(self._ns, {}).get(_key(entry["id"]))
                    event["fullDocument"] = copy.deepcopy(current)
                self.resume_token = event["_id"]
                return event
            self.resume_token = {"_data": self._hw_token()}
            return None


class FakeDatabase:
    def __init__(self, server: _Server, name: str):
        self._s, self.name = server, name

    def __getitem__(self, collection: str) -> FakeCollection:
        return FakeCollection(self._s, self.name, collection)

    def get_collection(self, collection: str, read_concern=None,
                       read_preference=None) -> FakeCollection:
        coll = FakeCollection(self._s, self.name, collection)
        coll.read_concern = read_concern
        self._s.read_preferences.append(getattr(read_preference, "name", None))
        return coll

    def create_collection(self, name: str, viewOn: Optional[str] = None, pipeline=None):  # pylint: disable=invalid-name
        del pipeline
        with self._s.lock:
            self._s.types[(self.name, name)] = "view" if viewOn else "collection"
            self._s.data.setdefault((self.name, name), {})
        return self[name]

    def command(self, name: str, collection: str):
        assert name == "collStats"
        return {"ns": f"{self.name}.{collection}", "sharded": False}

    def list_collections(self, filter=None, authorizedCollections=None, nameOnly=None):  # pylint: disable=redefined-builtin,invalid-name
        del authorizedCollections, nameOnly
        flt = filter or {}
        with self._s.lock:
            out = [{"name": c, "type": t} for (d, c), t in self._s.types.items() if d == self.name]
        if "type" in flt:
            out = [c for c in out if c["type"] == flt["type"]]
        if "name" in flt:
            out = [c for c in out if c["name"] == flt["name"]]
        return iter(out)


class FakeMongoClient:
    """Drop-in for ``pymongo.MongoClient`` as used by the connector."""

    _servers: dict[str, _Server] = {}

    def __init__(self, uri: str = "fake://default", **_kwargs):
        self._s = FakeMongoClient._servers.setdefault(uri, _Server())

    def __getitem__(self, name: str) -> FakeDatabase:
        return FakeDatabase(self._s, name)

    def list_database_names(self) -> list[str]:
        with self._s.lock:
            return sorted({d for d, _ in self._s.types})

    def close(self):
        pass
