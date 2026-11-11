"""MongoDB connector: initial snapshot + change stream CDC (with deletes).

One table per collection. The initial load pages through the collection by
``_id``; the change stream then picks up from a resume token taken *before*
the snapshot started, so nothing that changes during the snapshot is lost.
Rows carry the change stream resume token as the ordering key, which lets the
pipeline's ``apply_changes`` resolve snapshot rows, updates and deletes that
arrive out of order (for example, the separate delete flow).

See README.md for configuration, required MongoDB privileges and limits.
"""

from __future__ import annotations

import time
from typing import Any, Iterator, Optional

from bson.errors import InvalidBSON
from bson.timestamp import Timestamp
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect, SupportsNamespaces
from databricks.labs.community_connector.sources.mongodb.mongodb_auth import MongoClientFactory
from databricks.labs.community_connector.sources.mongodb.mongodb_schemas import (
    CURSOR_FIELD,
    DOCUMENT_FORMATS,
    PRIMARY_KEY,
    table_schema,
)
from databricks.labs.community_connector.sources.mongodb.mongodb_utils import (
    CANONICAL,
    OFFSET_VERSION,
    dumps,
    encode_id,
    loads,
    parse_choice,
    parse_int,
    timestamp_to_datetime,
    to_utc,
    token_data,
)

INGESTION_TYPES = ("cdc_with_deletes", "cdc", "append", "snapshot")
START_MODES = ("snapshot", "latest", "timestamp")
FULL_DOCUMENT_MODES = ("updateLookup", "whenAvailable", "required")
SYSTEM_DATABASES = ("admin", "config", "local")

# Change stream events that end the stream for a collection. They are always
# matched so the connector fails loudly instead of silently stopping.
TERMINAL_EVENTS = ("drop", "rename", "dropDatabase", "invalidate")
UPSERT_EVENTS = ("insert", "update", "replace")
DELETE_EVENTS = ("delete",)

DEFAULT_MAX_RECORDS_PER_BATCH = 10_000
DEFAULT_SNAPSHOT_BATCH_SIZE = 10_000
DEFAULT_MAX_AWAIT_MS = 1_000
DEFAULT_DELETE_LOOKBACK_SECONDS = 86_400
DEFAULT_IDLE_ADVANCE_SECONDS = 300
CLOCK_SKEW_SECONDS = 5
MIN_LOOKBACK_SECONDS = 60
SNAPSHOT_SEQUENCE = "0"
REPLAY_MAX_EMPTY_POLLS = 30

# MongoDB server error codes
CHANGE_STREAM_HISTORY_LOST = 286
CHANGE_STREAM_FATAL_ERROR = 280
CHANGE_STREAM_NOT_SUPPORTED = 40573  # standalone server

TABLE_OPTION_KEYS = (
    "database",
    "collection",
    "ingestion_type",
    "start_mode",
    "start_at_operation_time",
    "full_document",
    "document_format",
    "max_records_per_batch",
    "snapshot_batch_size",
    "max_await_time_ms",
    "delete_lookback_seconds",
    "idle_advance_seconds",
)


class _TableConfig:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """Validated per-table options."""

    def __init__(self, connector: "MongodbLakeflowConnect", table_name: str,
                 table_options: dict[str, str]):
        opts = {**connector.default_table_options, **(table_options or {})}
        database = opts.get("database") or connector.default_database
        # ``collection`` lets the table name be an ASCII alias. The pipeline
        # builds view names from the table name, and those must be plain
        # identifiers, so collections with multibyte or special characters in
        # their name are ingested as source_table=<alias> + collection=<name>.
        collection = opts.get("collection") or table_name
        if not database:
            # Fall back to "<db>.<collection>" only when no database is configured.
            # Collection names may contain dots, so the explicit option is preferred.
            if "." not in table_name:
                raise ValueError(
                    f"table '{table_name}': set the 'database' table option (or the "
                    "'database' connection parameter), or name the table '<database>.<collection>'"
                )
            database, collection = collection.split(".", 1)
        self.database = database
        self.collection = collection
        self.namespace = f"{database}.{collection}"
        self.ingestion_type = parse_choice(
            opts.get("ingestion_type"), "cdc_with_deletes", "ingestion_type", INGESTION_TYPES
        )
        self.start_mode = parse_choice(
            opts.get("start_mode"), "snapshot", "start_mode", START_MODES
        )
        self.start_at_operation_time = opts.get("start_at_operation_time")
        if self.start_mode == "timestamp" and not self.start_at_operation_time:
            raise ValueError(
                "start_mode=timestamp requires 'start_at_operation_time' (epoch seconds)"
            )
        self.full_document = parse_choice(
            opts.get("full_document"), "updateLookup", "full_document", FULL_DOCUMENT_MODES
        )
        self.document_format = parse_choice(
            opts.get("document_format"), "variant", "document_format", DOCUMENT_FORMATS
        )
        self.max_records = parse_int(
            opts.get("max_records_per_batch"), DEFAULT_MAX_RECORDS_PER_BATCH,
            "max_records_per_batch"
        )
        self.snapshot_batch_size = parse_int(
            opts.get("snapshot_batch_size"), DEFAULT_SNAPSHOT_BATCH_SIZE, "snapshot_batch_size"
        )
        self.max_await_ms = parse_int(
            opts.get("max_await_time_ms"), DEFAULT_MAX_AWAIT_MS, "max_await_time_ms"
        )
        self.delete_lookback = parse_int(
            opts.get("delete_lookback_seconds"), DEFAULT_DELETE_LOOKBACK_SECONDS,
            "delete_lookback_seconds", minimum=0
        )
        self.idle_advance = parse_int(
            opts.get("idle_advance_seconds"), DEFAULT_IDLE_ADVANCE_SECONDS,
            "idle_advance_seconds", minimum=0
        )


class MongodbLakeflowConnect(LakeflowConnect, SupportsNamespaces):
    """Lakeflow Connect implementation for MongoDB (replica sets and sharded clusters)."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._factory = MongoClientFactory(options)
        self.default_database = options.get("database") or None
        self.default_table_options = {
            k: v for k, v in options.items() if k in TABLE_OPTION_KEYS and k != "database"
        }
        self._client = None
        # Upper bound (cluster time) for change events read in the current
        # trigger, per (namespace, flow). See _read_stream.
        self._caps: dict[tuple[str, str], Timestamp] = {}
        self._known: set[str] = set()

    # The connector is pickled to Python workers: never ship a live client.
    def __getstate__(self):
        state = self.__dict__.copy()
        state["_client"] = None
        state["_caps"] = {}
        state["_known"] = set()
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    # ------------------------------------------------------------------ client

    def _get_client(self):
        if self._client is not None and self._factory.credentials_expire_soon():
            # New temporary AWS keys: the old client keeps authenticated sockets,
            # but any reconnect would fail once the keys expire.
            self._client.close()
            self._client = None
        if self._client is None:
            self._client = self._factory.create()
        return self._client

    def _collection(self, cfg: _TableConfig):
        return self._get_client()[cfg.database][cfg.collection]

    @staticmethod
    def _now() -> Timestamp:
        """The current time as a cluster-time bound.

        Taken from the local clock rather than from the server: some managed
        tiers (for example Atlas shared clusters) do not return
        ``operationTime``. The value only bounds how far one trigger reads and
        where the delete flow starts. It is rounded up and padded by
        ``CLOCK_SKEW_SECONDS`` so that every change made before the trigger
        started is read in that trigger, even if the server clock runs ahead.
        """
        return Timestamp(int(time.time()) + 1 + CLOCK_SKEW_SECONDS, 0)

    # --------------------------------------------------------------- catalog

    def list_namespaces(self, prefix: Optional[list[str]] = None) -> list[list[str]]:
        if prefix:
            return []  # databases have no child namespaces
        if self.default_database:
            return [[self.default_database]]
        names = self._get_client().list_database_names()
        return [[n] for n in names if n not in SYSTEM_DATABASES]

    def list_tables_in_namespace(self, namespace: list[str]) -> list[str]:
        if len(namespace) != 1:
            return []
        return self._list_collections(namespace[0])

    def _list_collections(self, database: str) -> list[str]:
        db = self._get_client()[database]
        # Views and time series collections cannot be watched by a change stream.
        cursor = db.list_collections(filter={"type": "collection"}, authorizedCollections=True,
                                     nameOnly=True)
        return sorted(c["name"] for c in cursor if not c["name"].startswith("system."))

    def list_tables(self) -> list[str]:
        if self.default_database:
            return self._list_collections(self.default_database)
        tables = []
        for (database,) in self.list_namespaces():
            tables.extend(f"{database}.{c}" for c in self._list_collections(database))
        return tables

    def _config(self, table_name: str, table_options: dict[str, str]) -> _TableConfig:
        cfg = _TableConfig(self, table_name, table_options)
        if cfg.namespace not in self._known:
            found = list(self._get_client()[cfg.database].list_collections(
                filter={"name": cfg.collection}, authorizedCollections=True, nameOnly=True))
            if not found:
                raise ValueError(
                    f"collection '{cfg.collection}' does not exist in database '{cfg.database}' "
                    "(or the user is not allowed to list it)"
                )
            if found[0].get("type", "collection") != "collection":
                raise ValueError(
                    f"'{cfg.namespace}' is a {found[0].get('type')}; only regular collections "
                    "have a change stream"
                )
            self._known.add(cfg.namespace)
        return cfg

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        cfg = self._config(table_name, table_options)
        return table_schema(cfg.document_format)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        cfg = self._config(table_name, table_options)
        if cfg.ingestion_type == "append":
            return {"cursor_field": CURSOR_FIELD, "ingestion_type": "append"}
        return {
            "primary_keys": [PRIMARY_KEY],
            "cursor_field": CURSOR_FIELD,
            "ingestion_type": cfg.ingestion_type,
        }

    # ------------------------------------------------------------------ reads

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        cfg = self._config(table_name, table_options)
        if cfg.ingestion_type == "snapshot":
            return self._read_full_snapshot(cfg), None
        events = UPSERT_EVENTS + (DELETE_EVENTS if cfg.ingestion_type == "append" else ())
        return self._read_incremental(cfg, start_offset or {}, "upsert", events)

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        cfg = self._config(table_name, table_options)
        return self._read_incremental(cfg, start_offset or {}, "delete", DELETE_EVENTS)

    def read_table_range(self, table_name: str, start_offset: dict, end_offset: dict,
                         table_options: dict[str, str], deletes: bool = False) -> Iterator[dict]:
        """Re-read exactly the batch ``(start_offset, end_offset]``.

        Used by :class:`MongodbStreamReader` when Spark replays a batch that was
        planned but not committed (after a restart). The generic reader re-reads
        from ``start_offset`` without a bound, which in ``append`` mode can write
        events of the next batch twice.
        """
        cfg = self._config(table_name, table_options)
        if cfg.ingestion_type == "snapshot":
            return self._read_full_snapshot(cfg)
        flow = "delete" if deletes else "upsert"
        if deletes:
            events = DELETE_EVENTS
        else:
            events = UPSERT_EVENTS + (DELETE_EVENTS if cfg.ingestion_type == "append" else ())
        records, _ = self._read_incremental(cfg, start_offset or {}, flow, events, end=end_offset)
        return records

    def _read_incremental(self, cfg: _TableConfig, start: dict, flow: str,
                          events: tuple[str, ...],
                          end: Optional[dict] = None) -> tuple[Iterator[dict], dict]:
        if not start:
            if end is not None:
                return iter([]), end  # the first batch only fixes the starting point
            return iter([]), self._initial_offset(cfg, flow)
        self._check_offset(cfg, start)
        if start["phase"] == "snapshot":
            return self._read_snapshot_page(cfg, start, end)
        return self._read_stream(cfg, start, flow, events, end)

    def _check_offset(self, cfg: _TableConfig, offset: dict) -> None:
        if offset.get("v") != OFFSET_VERSION:
            raise ValueError(f"unsupported offset version in {offset!r}; run a full refresh")
        if offset.get("ns") != cfg.namespace:
            raise ValueError(
                f"offset belongs to '{offset.get('ns')}' but the table now reads '{cfg.namespace}'. "
                "Changing the source collection of a table needs a full refresh."
            )

    def _offset(self, cfg: _TableConfig, phase: str, token: str, **extra: Any) -> dict:
        return {"v": OFFSET_VERSION, "ns": cfg.namespace, "phase": phase, "token": token, **extra}

    def _current_token(self, cfg: _TableConfig) -> str:
        """A resume token for 'now', without consuming any event.

        The stream matches nothing, so the token returned is the server's
        post-batch resume token: every change up to it has been scanned and
        none was skipped by consuming it here.
        """
        import pymongo.errors  # pylint: disable=import-outside-toplevel

        try:
            with self._collection(cfg).watch(pipeline=[{"$match": {"_lakeflow_never": True}}],
                                             max_await_time_ms=1) as stream:
                stream.try_next()
                return token_data(stream.resume_token)
        except pymongo.errors.OperationFailure as exc:
            if exc.code == CHANGE_STREAM_NOT_SUPPORTED:
                raise RuntimeError(
                    "Change streams need a replica set or a sharded cluster; standalone servers "
                    "are not supported. Use ingestion_type=snapshot for a standalone server."
                ) from exc
            raise

    def _initial_offset(self, cfg: _TableConfig, flow: str) -> dict:
        if cfg.start_mode == "timestamp":
            # Resolved by the first stream read (start_at_operation_time).
            start_at = int(float(cfg.start_at_operation_time))
            return self._offset(cfg, "stream", None, start_at=start_at)
        if flow == "delete":
            # The delete flow is a separate stream that starts slightly after the
            # upsert flow takes its snapshot token. Start it a little in the past:
            # deletes older than the snapshot carry a smaller sequence than the
            # snapshot rows, so apply_changes ignores them, and none in the gap
            # between the two flows is lost.
            if cfg.delete_lookback == 0:
                return self._offset(cfg, "stream", self._current_token(cfg))
            now = self._now()
            start_at = max(now.time - cfg.delete_lookback, 1)
            return self._offset(cfg, "stream", None, start_at=start_at,
                                lookback=True)
        token = self._current_token(cfg)
        if cfg.start_mode == "latest":
            return self._offset(cfg, "stream", token)
        return self._offset(cfg, "snapshot", token, last_id=None)

    def _read_snapshot_page(self, cfg: _TableConfig, start: dict,
                            end: Optional[dict] = None) -> tuple[Iterator[dict], dict]:
        limit = min(cfg.max_records, cfg.snapshot_batch_size)
        conditions = []
        if start.get("last_id") is not None:
            last = loads(start["last_id"], CANONICAL)
            # $expr compares across BSON types in MongoDB's sort order, so a
            # collection whose _id mixes ObjectId, strings and numbers is paged
            # without skipping a type (a plain {$gt} only matches the same type).
            conditions.append({"$gt": ["$_id", {"$literal": last}]})
        if end is not None:
            # Replay: stop at the last _id of the original page (also for the
            # final page, whose end offset is already in the stream phase).
            end_id = (end.get("last_id") if end.get("phase") == "snapshot"
                      else end.get("snapshot_last_id"))
            if end_id is None:
                return iter([]), end
            conditions.append({"$lte": ["$_id", {"$literal": loads(end_id, CANONICAL)}]})
            limit = 0
        query = {"$expr": {"$and": conditions}} if conditions else {}
        docs = self._guard_decode(
            cfg, lambda: list(self._collection(cfg).find(query).sort("_id", 1).limit(limit))
        )
        if end is not None:
            return iter([self._snapshot_row(cfg, d, start["token"]) for d in docs]), end
        rows = [self._snapshot_row(cfg, d, start["token"]) for d in docs]
        if len(docs) < limit:
            # Snapshot done: stream from the token taken before the snapshot began.
            # The last _id is kept so that a replay of this page can stop there.
            final_id = dumps(docs[-1]["_id"], CANONICAL) if docs else start.get("last_id")
            return iter(rows), self._offset(cfg, "stream", start["token"],
                                            snapshot_last_id=final_id)
        last_id = dumps(docs[-1]["_id"], CANONICAL)
        return iter(rows), self._offset(cfg, "snapshot", start["token"], last_id=last_id)

    def _read_full_snapshot(self, cfg: _TableConfig) -> Iterator[dict]:
        # apply_changes_from_snapshot does not order by _cdc_sequence, and this
        # mode must work without change streams (standalone servers, users
        # without the changeStream privilege), so no resume token is taken.
        token = SNAPSHOT_SEQUENCE
        cursor = self._collection(cfg).find({}).sort("_id", 1).batch_size(cfg.snapshot_batch_size)
        try:
            for doc in cursor:
                yield self._snapshot_row(cfg, doc, token)
        except (InvalidBSON, UnicodeDecodeError) as exc:
            raise self._decode_error(cfg, exc) from exc

    def _snapshot_row(self, cfg: _TableConfig, doc: dict, token: str) -> dict:
        return {
            "_id": encode_id(doc["_id"]),
            "document": dumps(doc),
            "_cdc_operation": "snapshot",
            "_cdc_sequence": token,
            "_cdc_cluster_time": None,
            "_cdc_wall_time": None,
            "_cdc_namespace": cfg.namespace,
        }

    def _read_stream(self, cfg: _TableConfig, start: dict, flow: str,
                     events: tuple[str, ...],
                     end: Optional[dict] = None) -> tuple[Iterator[dict], dict]:
        """Read one micro-batch of change events after ``start``.

        With ``end`` (a replay of a batch that was planned but not committed),
        read exactly the events up to and including ``end``, so the replayed
        batch has the same rows as the original one.
        """
        import pymongo.errors  # pylint: disable=import-outside-toplevel

        if end is not None and (end.get("phase") != "stream" or not end.get("token")
                                or end.get("idle") or end.get("token") == start.get("token")):
            return iter([]), end  # the batch had no events
        cap_key = (cfg.namespace, flow)
        cap = self._caps.get(cap_key)
        if cap is None:
            # Read only changes that existed when this trigger started, so that
            # Trigger.AvailableNow terminates even under constant writes.
            cap = self._caps[cap_key] = self._now()

        watch_kwargs = self._watch_kwargs(cfg, start, flow, events)

        rows: list[dict] = []
        last_token: Optional[str] = None
        drained = False
        pbrt: Optional[str] = None
        empty_polls = 0
        try:
            with self._collection(cfg).watch(**watch_kwargs) as stream:
                while end is not None or len(rows) < cfg.max_records:
                    event = stream.try_next()
                    if event is None:
                        empty_polls += 1
                        replaying = end is not None and stream.alive
                        if replaying and empty_polls < REPLAY_MAX_EMPTY_POLLS:
                            continue  # replay: the events up to `end` exist; keep polling
                        drained = True
                        pbrt = token_data(stream.resume_token)
                        break
                    token = token_data(event["_id"])
                    if end is not None:
                        if token > end["token"]:
                            break
                    elif event.get("clusterTime") and event["clusterTime"] > cap:
                        break  # newer than this trigger; read it next time
                    self._raise_if_terminal(cfg, event)
                    last_token = token
                    row = self._change_row(cfg, event, flow)
                    if row is not None:
                        rows.append(row)
                    if end is not None and token == end["token"]:
                        break
        except pymongo.errors.OperationFailure as exc:
            if (exc.code == CHANGE_STREAM_HISTORY_LOST and not start.get("token")
                    and start.get("lookback")):
                return iter([]), self._oldest_available_start(cfg, start, flow)
            if exc.code in (CHANGE_STREAM_HISTORY_LOST, CHANGE_STREAM_FATAL_ERROR):
                raise RuntimeError(
                    f"The change stream position of {cfg.namespace} ({flow} flow) is no longer in "
                    "the oplog (the pipeline was stopped longer than the oplog window). Run a full "
                    "refresh of this table to take a new snapshot, and consider a larger oplog."
                ) from exc
            raise
        except (InvalidBSON, UnicodeDecodeError) as exc:
            raise self._decode_error(cfg, exc) from exc

        if end is not None:
            return iter(rows), end
        if last_token is not None:
            return iter(rows), self._offset(cfg, "stream", last_token)

        return self._empty_poll(cfg, start, cap_key, drained, pbrt)

    @staticmethod
    def _watch_kwargs(cfg: _TableConfig, start: dict, flow: str,
                      events: tuple[str, ...]) -> dict[str, Any]:
        pipeline = [{"$match": {"operationType": {"$in": list(events + TERMINAL_EVENTS)}}}]
        kwargs: dict[str, Any] = {
            "pipeline": pipeline,
            "max_await_time_ms": cfg.max_await_ms,
            "batch_size": min(cfg.max_records, 1000),
        }
        if start.get("token"):
            kwargs["resume_after"] = {"_data": start["token"]}
        else:
            kwargs["start_at_operation_time"] = Timestamp(int(start["start_at"]), 0)
        if flow == "upsert":
            kwargs["full_document"] = cfg.full_document
        return kwargs

    def _empty_poll(self, cfg: _TableConfig, start: dict, cap_key: tuple[str, str],
                    drained: bool, pbrt: Optional[str]) -> tuple[Iterator[dict], dict]:
        """No new event up to the cap: decide the offset to report."""
        # Move the cap forward for the next trigger
        # (continuous mode) and report "no more data" for this one.
        self._caps[cap_key] = self._now()
        if not (drained and pbrt):
            return iter([]), start
        if not start.get("token"):
            # A position given as a time is turned into a token once it is read.
            return iter([]), self._offset(cfg, "stream", pbrt, at=self._caps[cap_key].time,
                                          idle=True)
        # On an idle collection, move the stored token forward now and then so
        # that a long pause does not leave it behind the oplog window. Never
        # twice in a row: the second empty poll must end Trigger.AvailableNow.
        idle_for = self._caps[cap_key].time - start.get("at", 0)
        if (cfg.idle_advance and not start.get("idle") and pbrt > start["token"]
                and idle_for >= cfg.idle_advance):
            return iter([]), self._offset(cfg, "stream", pbrt, at=self._caps[cap_key].time,
                                          idle=True)
        return iter([]), start

    def _oldest_available_start(self, cfg: _TableConfig, start: dict, flow: str) -> dict:
        """The look-back start is older than the oplog: find the oldest start that works.

        Jumping to "now" would skip deletes that are still in the oplog, so the
        oldest readable time is found by bisection between the failed start
        and now (to within ``MIN_LOOKBACK_SECONDS``).
        """
        import pymongo.errors  # pylint: disable=import-outside-toplevel

        low, high = int(start["start_at"]), self._now().time
        while high - low > MIN_LOOKBACK_SECONDS:
            mid = (low + high) // 2
            try:
                with self._collection(cfg).watch(start_at_operation_time=Timestamp(mid, 0),
                                                 pipeline=[{"$match": {"_lakeflow_never": True}}],
                                                 max_await_time_ms=1) as stream:
                    stream.try_next()
                high = mid
            except pymongo.errors.OperationFailure as exc:
                if exc.code != CHANGE_STREAM_HISTORY_LOST:
                    raise
                low = mid
        if self._now().time - high < MIN_LOOKBACK_SECONDS:
            raise RuntimeError(
                f"The oplog of {cfg.namespace} does not reach back even {MIN_LOOKBACK_SECONDS} "
                f"seconds, so the {flow} flow cannot start without missing changes."
            )
        return self._offset(cfg, "stream", None, start_at=high)

    @staticmethod
    def _raise_if_terminal(cfg: _TableConfig, event: dict) -> None:
        op = event.get("operationType")
        if op in TERMINAL_EVENTS:
            raise RuntimeError(
                f"The change stream of {cfg.namespace} ended with a '{op}' event (the collection "
                "was dropped or renamed). Run a full refresh after the collection is recreated."
            )

    def _change_row(self, cfg: _TableConfig, event: dict, flow: str) -> Optional[dict]:
        op = event["operationType"]
        key = event.get("documentKey") or {}
        if "_id" not in key:
            return None
        document = None
        if op != "delete":
            full = event.get("fullDocument")
            if full is None and cfg.full_document != "updateLookup" and flow == "upsert":
                # whenAvailable: the post-image can be missing (for example expired
                # by expireAfterSeconds). Fall back to the current document rather
                # than dropping the change; `required` makes the server fail instead.
                full = self._collection(cfg).find_one({"_id": key["_id"]})
            if full is not None:
                document = dumps(full)
            elif cfg.ingestion_type != "append":
                # updateLookup found nothing: the document was deleted after this
                # update. The delete event that follows removes the row.
                return None
            # In append mode the event is kept as a change-log entry; the
            # document is null because it no longer exists.
        return {
            "_id": encode_id(key["_id"]),
            "document": document if flow == "upsert" else None,
            "_cdc_operation": op,
            "_cdc_sequence": token_data(event["_id"]),
            "_cdc_cluster_time": timestamp_to_datetime(event.get("clusterTime")),
            "_cdc_wall_time": to_utc(event.get("wallTime")),
            "_cdc_namespace": cfg.namespace,
        }

    def _guard_decode(self, cfg: _TableConfig, fn):
        try:
            return fn()
        except (InvalidBSON, UnicodeDecodeError) as exc:
            raise self._decode_error(cfg, exc) from exc

    @staticmethod
    def _decode_error(cfg: _TableConfig, exc: Exception) -> RuntimeError:
        return RuntimeError(
            f"A document in {cfg.namespace} contains a string that is not valid UTF-8 ({exc}). "
            "Fix the data, or set the connection parameter unicode_decode_error_handler=replace "
            "to ingest it with U+FFFD replacement characters."
        )
