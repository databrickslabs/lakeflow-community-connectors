"""MongoDB community connector for Lakeflow Community Connectors.

Implements the ``LakeflowConnect`` interface over ``pymongo``:

- ``snapshot``         — full collection via ``find()`` (batch path, one call).
- ``cdc``              — gapless initial ``_id`` pagination, then change-stream
                          tail of insert/update/replace events.
- ``cdc_with_deletes`` — ``cdc`` plus ``read_table_deletes()`` over change-stream
                          delete events (pre-image when enabled on the collection).
- ``append``           — monotonic ``_id`` incremental, no change stream.

The full Mongo document lands as a single VARIANT column (emitted as a JSON
string per the framework's VARIANT materialization contract); metadata columns
carry CDC plumbing. Schema and metadata are deterministic — no live sampling, no
topology probing — so the connector passes the framework test harness.
"""

from __future__ import annotations

from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsNamespaces,
    SupportsPartition,
)
from databricks.labs.community_connector.sources.mongodb.mongodb_cdc import (
    current_cluster_time,
    read_change_batch,
    read_delete_batch,
)
from databricks.labs.community_connector.sources.mongodb.mongodb_schemas import (
    variant_bronze_schema,
)
from databricks.labs.community_connector.sources.mongodb.mongodb_utils import (
    COL_CLUSTER_TIME,
    COL_ID,
    VALID_INGESTION_TYPES,
    append_offset,
    append_row,
    client_options,
    decode_id,
    encode_id,
    is_same_offset,
    parse_options,
    parse_pii,
    resolve_db_collection,
    snapshot_row,
    system_db,
)


class MongodbLakeflowConnect(LakeflowConnect, SupportsNamespaces, SupportsPartition):
    """LakeflowConnect implementation for MongoDB."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        cfg = parse_options(options)
        self._uri = cfg["connection_uri"]
        self._database = cfg["database"]
        self._allowlist = cfg["database_allowlist"]
        self._client_opts = client_options(options)  # secondary reads + resilience
        self._pii = parse_pii(options)  # drop/hash PII fields before they land
        self._batch = int(options.get("max_records_per_batch", "1000"))
        self._idle_ms = int(options.get("cdc_idle_ms", "1000"))
        self._default_ingestion = options.get("ingestion_type", "cdc")
        self._delete_mode = options.get("delete_mode", "hard")  # hard (physical) | soft (flag)
        # STRING document by default (streaming-safe); VARIANT opt-in (batch only).
        self._document_as_variant = options.get("document_as_variant", "false").lower() == "true"
        self._client = None

    # ---- lazy, picklable client -------------------------------------------- #

    @property
    def client(self):
        """Lazily construct a ``MongoClient`` (importlib so the class pickles cleanly)."""
        if self._client is None:
            import importlib

            pymongo = importlib.import_module("pymongo")
            self._client = pymongo.MongoClient(self._uri, **self._client_opts)
        return self._client

    def __getstate__(self) -> dict:
        state = dict(self.__dict__)
        state["_client"] = None  # never pickle a live socket to executors
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._client = None

    # ---- discovery (SupportsNamespaces) ------------------------------------ #

    def list_namespaces(self, prefix: list[str] | None = None) -> list[list[str]]:
        if prefix:  # MongoDB namespaces are one level (database); no children under a db
            return []
        return [
            [d]
            for d in self.client.list_database_names()
            if not system_db(d) and (not self._allowlist or d in self._allowlist)
        ]

    def list_tables_in_namespace(self, namespace: list[str]) -> list[str]:
        if not namespace:
            return []
        return [
            c
            for c in self.client[namespace[0]].list_collection_names()
            if not c.startswith("system.")
        ]

    def list_tables(self) -> list[str]:
        if self._database:
            return self.list_tables_in_namespace([self._database])
        return [
            f"{ns[0]}.{coll}"
            for ns in self.list_namespaces()
            for coll in self.list_tables_in_namespace(ns)
        ]

    # ---- validation + schema + metadata (deterministic) -------------------- #

    def _validate_table(self, table_name: str, table_options: dict[str, str]) -> tuple[str, str]:
        """Resolve (db, collection) and verify it exists; raise for unknown tables."""
        db, coll = resolve_db_collection(table_name, table_options, self._database)
        if coll not in self.client[db].list_collection_names():
            raise ValueError(f"Table '{table_name}' not found in MongoDB database '{db}'")
        return db, coll

    def _ingestion_type(self, table_options: dict[str, str]) -> str:
        ingestion_type = table_options.get("ingestion_type", self._default_ingestion)
        if ingestion_type not in VALID_INGESTION_TYPES:
            raise ValueError(f"invalid ingestion_type '{ingestion_type}'")
        return ingestion_type

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name, table_options)
        return variant_bronze_schema(self._document_as_variant)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name, table_options)
        raw = self._ingestion_type(table_options)
        # Soft-delete: deletes ride the main stream as rows, so report plain "cdc"
        # to the framework (no physical-delete flow / read_table_deletes).
        reported = "cdc" if (raw == "cdc_with_deletes" and self._delete_mode == "soft") else raw
        return {
            "primary_keys": [COL_ID],
            "cursor_field": COL_CLUSTER_TIME,
            "ingestion_type": reported,
        }

    # ---- preflight (startup validation, mirrors Fivetran's setup test) ------ #

    def preflight(self, table_name: str, table_options: dict[str, str]) -> list[str]:
        """Best-effort startup checks. Raises on hard blockers; returns warnings.

        - CDC requires a replica set (change streams) — hard error if absent.
        - ``cdc_with_deletes`` (hard mode) wants ``changeStreamPreAndPostImages`` so
          deletes carry the pre-image — warn (not fail) if disabled.
        """
        db, coll = self._validate_table(table_name, table_options)
        ingestion_type = self._ingestion_type(table_options)
        warnings: list[str] = []
        if ingestion_type in ("cdc", "cdc_with_deletes"):
            try:
                is_replica_set = bool(self.client.admin.command("hello").get("setName"))
            except Exception:  # noqa: BLE001 — topology probe is best-effort
                is_replica_set = True  # don't block if we can't introspect
            if not is_replica_set:
                raise ValueError(
                    f"{table_name}: ingestion_type '{ingestion_type}' needs a replica set "
                    "(change streams are unavailable on a standalone mongod)."
                )
        if ingestion_type == "cdc_with_deletes" and self._delete_mode != "soft":
            if not self._preimages_enabled(db, coll):
                warnings.append(
                    f"{table_name}: changeStreamPreAndPostImages is not enabled — deletes "
                    "will be key-only (no pre-image). Enable it via collMod for full deletes."
                )
        return warnings

    def _preimages_enabled(self, db: str, coll: str) -> bool:
        try:
            infos = list(self.client[db].list_collections(filter={"name": coll}))
            opts = infos[0].get("options", {}) if infos else {}
            return bool(opts.get("changeStreamPreAndPostImages", {}).get("enabled"))
        except Exception:  # noqa: BLE001
            return False

    # ---- reads ------------------------------------------------------------- #

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        db, coll = self._validate_table(table_name, table_options)
        ingestion_type = self._ingestion_type(table_options)
        collection = self.client[db][coll]
        if ingestion_type == "snapshot":
            return self._read_snapshot(collection, db, coll), {}
        if ingestion_type == "append":
            return self._read_append(collection, db, coll, start_offset)
        include_deletes = ingestion_type == "cdc_with_deletes" and self._delete_mode == "soft"
        return self._read_cdc(collection, db, coll, start_offset, include_deletes)

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        db, coll = self._validate_table(table_name, table_options)
        collection = self.client[db][coll]
        return read_delete_batch(
            collection, db, coll, start_offset, self._batch, self._idle_ms, self._pii
        )

    # ---- SupportsPartition: parallel _id-range snapshot of large collections -- #

    def get_partitions(self, table_name: str, table_options: dict[str, str]) -> list[dict]:
        """Split a collection's _id keyspace into ranges for parallel snapshot reads.

        Uses ``$bucketAuto`` over ``_id`` (Atlas-safe; ``$splitVector`` is unavailable
        on Atlas). Returns half-open ``[lo, hi)`` ranges (first ``lo`` / last ``hi``
        unbounded) as JSON-safe descriptors. Only the batch/snapshot path partitions;
        CDC is a single change-stream tail.
        """
        db, coll = self._validate_table(table_name, table_options)
        n = max(1, int(table_options.get("num_partitions", "8")))
        buckets = list(
            self.client[db][coll].aggregate([{"$bucketAuto": {"groupBy": "$_id", "buckets": n}}])
        )
        if not buckets:
            return [{"lo": None, "hi": None}]
        mins = [b["_id"]["min"] for b in buckets]
        return [
            {
                "lo": encode_id(mins[i]) if i > 0 else None,
                "hi": encode_id(mins[i + 1]) if i + 1 < len(mins) else None,
            }
            for i in range(len(mins))
        ]

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Read one ``_id``-range partition (runs on a Spark executor)."""
        db, coll = resolve_db_collection(table_name, table_options, self._database)
        lo, hi = decode_id(partition.get("lo")), decode_id(partition.get("hi"))
        bounds = {}
        if lo is not None:
            bounds["$gte"] = lo
        if hi is not None:
            bounds["$lt"] = hi
        query = {"_id": bounds} if bounds else {}
        for doc in self.client[db][coll].find(query).sort("_id", 1):
            yield snapshot_row(doc, db, coll, pii=self._pii)

    # ---- snapshot (batch path: one call, offset ignored, lazy cursor) ------- #

    def _read_snapshot(self, collection, db: str, coll: str) -> Iterator[dict]:
        for doc in collection.find().sort("_id", 1):
            yield snapshot_row(doc, db, coll, pii=self._pii)

    # ---- append: bounded _id-cursor incremental ---------------------------- #

    def _read_append(self, collection, db, coll, start_offset):
        last_enc = (start_offset or {}).get("last_id")
        query = {}
        last_id = decode_id(last_enc)
        if last_id is not None:
            query = {"_id": {"$gt": last_id}}
        docs = list(collection.find(query).sort("_id", 1).limit(self._batch))
        if not docs:
            return iter([]), (start_offset or {})
        rows = [append_row(d, db, coll, pii=self._pii) for d in docs]
        end = append_offset(encode_id(docs[-1]["_id"]))
        if is_same_offset(start_offset, end):
            return iter([]), start_offset
        return iter(rows), end

    # ---- cdc: gapless initial pagination -> change-stream tail -------------- #

    def _read_cdc(self, collection, db, coll, start_offset, include_deletes=False):
        offset = start_offset or {}
        phase = offset.get("phase")
        if not phase:
            t0 = current_cluster_time(collection.database.client)
            return self._cdc_initial(
                collection, db, coll, {"phase": "initial", "last_id": None, "t0": t0}
            )
        if phase == "initial":
            return self._cdc_initial(collection, db, coll, offset)
        return read_change_batch(
            collection, db, coll, offset, self._batch, self._idle_ms, self._pii, include_deletes
        )

    def _cdc_initial(self, collection, db, coll, offset):
        t0 = offset.get("t0")
        last_id = decode_id(offset.get("last_id"))
        query = {"_id": {"$gt": last_id}} if last_id is not None else {}
        docs = list(collection.find(query).sort("_id", 1).limit(self._batch))
        if not docs:
            # initial load drained — hand off to the change-stream tail from t0
            return iter([]), {"phase": "stream", "resume_token": None, "t0": t0}
        rows = [snapshot_row(d, db, coll, cluster_time=t0, pii=self._pii) for d in docs]
        return iter(rows), {"phase": "initial", "last_id": encode_id(docs[-1]["_id"]), "t0": t0}
