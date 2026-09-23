"""MongoDB (Atlas) connector for Lakeflow Community Connectors.

Each document is emitted through a stable envelope schema:

- ``_id``: the document identifier rendered as a stable string.
- ``document``: the full document as VARIANT, serialised from MongoDB
  Extended JSON (Relaxed mode) so BSON types such as ObjectId,
  Decimal128, dates and binary data are preserved.
- ``event_time``: ``clusterTime`` of the oplog event (connector init
  time for bootstrap rows), used as the sequencing key for SCD2.

This keeps the Spark schema stable regardless of how heterogeneous the
documents in a collection are, which is the common case in MongoDB.

Every collection is ingested via change streams (``cdc_with_deletes``).
The first upsert run dumps the collection (paginated) after capturing a
resume token, then later triggers only watch. Inserts, updates and
replaces are emitted even when ``_id`` does not change; deletes are
returned from ``read_table_deletes``. Requires a replica set or Atlas
cluster.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Iterator, Optional

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
    VariantType,
)

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect

if TYPE_CHECKING:
    from pymongo import MongoClient

# MongoDB stores internal collections under the ``system.*`` prefix; they
# are not user data and must not be surfaced as ingestible tables.
_SYSTEM_COLLECTION_PREFIX = "system."

# Connection timeouts (milliseconds). Always bounded so a misconfigured
# URI or unreachable cluster fails fast instead of hanging the pipeline.
_SERVER_SELECTION_TIMEOUT_MS = 20_000
_CONNECT_TIMEOUT_MS = 20_000

# Default cap on documents returned per read_table call.
_DEFAULT_MAX_RECORDS_PER_BATCH = 1000

_ID_FIELD = "_id"
_DOCUMENT_FIELD = "document"
_EVENT_TIME_FIELD = "event_time"
_RESUME_TOKEN_KEY = "resume_token"
_PHASE_KEY = "phase"
_BOOTSTRAP_PHASE = "bootstrap"
_SNAPSHOT_ID_KEY = "snapshot_id"

_UPSERT_OPERATION_TYPES = ["insert", "update", "replace"]
_DELETE_OPERATION_TYPES = ["delete"]
_INVALIDATING_OPERATION_TYPES = frozenset({"drop", "dropDatabase", "rename", "invalidate"})

# Options removed when the connector became change-stream / VARIANT only.
_REMOVED_TABLE_OPTIONS = (
    "cdc_mode",
    "cursor_field",
    "cursor_type",
    "start_timestamp",
)

# Short await so an empty getMore returns instead of hanging the microbatch.
_CHANGE_STREAM_MAX_AWAIT_MS = 1000


class MongoDBLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for MongoDB / MongoDB Atlas."""

    def __init__(self, options: dict[str, str]) -> None:
        """Initialise the connector.

        Args:
            options: Connection parameters:
                - connection_uri: MongoDB connection string, e.g.
                  ``mongodb+srv://user:pass@cluster.mongodb.net`` (required).
                - database: Name of the database to read from (required).

        Raises:
            ValueError: If a required parameter is missing.
        """
        super().__init__(options)

        self._connection_uri = options.get("connection_uri")
        if not self._connection_uri:
            raise ValueError("Missing required parameter 'connection_uri'")

        self._database = options.get("database")
        if not self._database:
            raise ValueError("Missing required parameter 'database'")

        # Cap the change-stream drain at init time so a single trigger only
        # emits events that existed when the connector started. The next
        # trigger builds a fresh instance with a newer cap and picks up the
        # rest, which is what makes CDC reads converge under
        # Trigger.AvailableNow.
        self._init_dt = datetime.now(timezone.utc)

    def _client(self) -> "MongoClient":
        """Create a new MongoClient.

        A fresh client is created per call rather than cached on the
        instance: Spark serialises the connector to ship it to executors,
        and a live ``MongoClient`` (which owns background threads and
        sockets) is not picklable. Callers own the returned client's
        lifecycle and must close it.
        """
        from pymongo import MongoClient

        return MongoClient(
            self._connection_uri,
            serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS,
            connectTimeoutMS=_CONNECT_TIMEOUT_MS,
        )

    def list_tables(self) -> list[str]:
        """Return all user collections in the configured database."""
        client = self._client()
        try:
            db = client[self._database]
            return [
                name
                for name in db.list_collection_names()
                if not name.startswith(_SYSTEM_COLLECTION_PREFIX)
            ]
        finally:
            client.close()

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """Return the envelope schema used by every collection."""
        self._validate_table(table_name)
        self._reject_removed_options(table_options)
        return StructType(
            [
                StructField(_ID_FIELD, StringType(), False),
                StructField(_DOCUMENT_FIELD, VariantType(), True),
                StructField(_EVENT_TIME_FIELD, TimestampType(), False),
            ]
        )

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Return change-stream metadata for every collection."""
        self._validate_table(table_name)
        self._reject_removed_options(table_options)
        return {
            "primary_keys": [_ID_FIELD],
            "cursor_field": _EVENT_TIME_FIELD,
            "ingestion_type": "cdc_with_deletes",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read one change-stream upsert microbatch, or a bootstrap page."""
        self._validate_table(table_name)
        self._reject_removed_options(table_options)
        return self._read_change_stream(table_name, start_offset, table_options, for_deletes=False)

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read delete change-stream events for ``cdc_with_deletes``.

        Independent of ``read_table``: the framework checkpoints this flow
        with its own resume token.
        """
        self._validate_table(table_name)
        self._reject_removed_options(table_options)
        return self._read_change_stream(table_name, start_offset, table_options, for_deletes=True)

    def _read_change_stream(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        for_deletes: bool,
    ) -> tuple[Iterator[dict], dict]:
        """Drain one change-stream microbatch, or bootstrap the collection.

        On the first upsert read (empty offset) a resume token is captured
        from ``watch()`` and the collection is dumped in ``_id`` pages.
        Later calls only watch. Deletes never dump. ``try_next`` returns
        ``None`` after ``max_await_time_ms`` with no event, which is what
        lets AvailableNow terminate instead of blocking forever on
        ``watch()``.
        """
        from pymongo.errors import OperationFailure, PyMongoError

        start_offset = start_offset or {}
        if self._is_bootstrap(start_offset, for_deletes):
            return self._bootstrap_change_stream(table_name, start_offset, table_options)

        max_records = self._resolve_max_records(table_options)
        batch_size = self._resolve_batch_size(table_options)
        operations = _DELETE_OPERATION_TYPES if for_deletes else _UPSERT_OPERATION_TYPES

        watch_kwargs: dict = {
            "pipeline": [{"$match": {"operationType": {"$in": operations}}}],
            "max_await_time_ms": _CHANGE_STREAM_MAX_AWAIT_MS,
        }
        if not for_deletes:
            watch_kwargs["full_document"] = "updateLookup"
        if batch_size:
            watch_kwargs["batch_size"] = batch_size

        token_str = start_offset.get(_RESUME_TOKEN_KEY)
        if token_str:
            watch_kwargs["resume_after"] = self._str_to_resume_token(token_str)
        else:
            watch_kwargs["start_at_operation_time"] = self._change_stream_start_timestamp()

        client = self._client()
        try:
            collection = client[self._database][table_name]
            try:
                stream = collection.watch(**watch_kwargs)
            except OperationFailure as exc:
                raise self._watch_failure(exc) from exc
            except PyMongoError as exc:
                raise ValueError(f"MongoDB change stream failed: {exc}") from exc
            try:
                return self._drain_change_stream(stream, start_offset, max_records, for_deletes)
            except PyMongoError as exc:
                raise ValueError(
                    f"MongoDB change stream could not be resumed: {exc}. "
                    "If the resume token is stale, full-refresh the destination table."
                ) from exc
            finally:
                stream.close()
        finally:
            client.close()

    @staticmethod
    def _is_bootstrap(start_offset: dict, for_deletes: bool) -> bool:
        """True when the upsert path should dump the collection."""
        if for_deletes:
            return False
        if (start_offset or {}).get(_PHASE_KEY) == _BOOTSTRAP_PHASE:
            return True
        if start_offset.get(_RESUME_TOKEN_KEY):
            return False
        return True

    def _bootstrap_change_stream(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Capture a resume token, then emit one page of the collection dump."""
        max_records = self._resolve_max_records(table_options)
        batch_size = self._resolve_batch_size(table_options)
        token_str = start_offset.get(_RESUME_TOKEN_KEY)
        snapshot_id = start_offset.get(_SNAPSHOT_ID_KEY)

        client = self._client()
        try:
            collection = client[self._database][table_name]
            if not token_str:
                token_str = self._capture_resume_token(collection)
            records, last_id, exhausted = self._scan_bootstrap_page(
                collection, snapshot_id, max_records, batch_size
            )
        finally:
            client.close()

        if not exhausted and last_id is not None:
            end_offset = {
                _PHASE_KEY: _BOOTSTRAP_PHASE,
                _RESUME_TOKEN_KEY: token_str,
                _SNAPSHOT_ID_KEY: self._token_to_str(last_id),
            }
            return iter(records), end_offset
        return iter(records), {_RESUME_TOKEN_KEY: token_str}

    def _capture_resume_token(self, collection) -> str:
        """Open a watch, populate the post-batch token, then close it.

        The dump runs after this so updates during the scan are still in
        the oplog and are replayed once the offset becomes streaming-only.
        """
        from bson.timestamp import Timestamp
        from pymongo.errors import OperationFailure, PyMongoError

        watch_kwargs = {
            "pipeline": [{"$match": {"operationType": {"$in": _UPSERT_OPERATION_TYPES}}}],
            "max_await_time_ms": _CHANGE_STREAM_MAX_AWAIT_MS,
            "full_document": "updateLookup",
            "start_at_operation_time": Timestamp(int(self._init_dt.timestamp()), 1),
        }
        try:
            stream = collection.watch(**watch_kwargs)
        except OperationFailure as exc:
            raise self._watch_failure(exc) from exc
        except PyMongoError as exc:
            raise ValueError(f"MongoDB change stream failed: {exc}") from exc
        try:
            stream.try_next()
            token = stream.resume_token
        except PyMongoError as exc:
            raise ValueError(
                f"MongoDB change stream could not be resumed: {exc}. "
                "If the resume token is stale, full-refresh the destination table."
            ) from exc
        finally:
            stream.close()
        if token is None:
            raise ValueError(
                "MongoDB did not return a change stream resume token. "
                "Retry the pipeline; if it persists, the cluster may not "
                "support change streams."
            )
        return self._token_to_str(token)

    def _scan_bootstrap_page(
        self,
        collection,
        snapshot_id: Optional[str],
        max_records: int,
        batch_size: int,
    ) -> tuple[list[dict], object, bool]:
        """Read one ``_id``-ordered page of the collection for bootstrap."""
        query: dict = {}
        if snapshot_id:
            query = {_ID_FIELD: {"$gt": self._str_to_resume_token(snapshot_id)}}
        db_cursor = collection.find(query).sort(_ID_FIELD, 1).limit(max_records)
        if batch_size:
            db_cursor = db_cursor.batch_size(batch_size)
        records: list[dict] = []
        last_id = None
        for document in db_cursor:
            records.append(self._document_to_record(document, self._init_dt))
            last_id = document[_ID_FIELD]
        exhausted = len(records) < max_records
        return records, last_id, exhausted

    def _drain_change_stream(
        self,
        stream,
        start_offset: dict,
        max_records: int,
        for_deletes: bool,
    ) -> tuple[Iterator[dict], dict]:
        """Consume events up to the init-time cap and ``max_records``."""
        records: list[dict] = []
        last_token = None
        while len(records) < max_records:
            event = stream.try_next()
            if event is None:
                break
            operation = event.get("operationType")
            if operation in _INVALIDATING_OPERATION_TYPES:
                raise ValueError(
                    f"MongoDB change stream was invalidated by {operation!r}. "
                    "The stream cannot be resumed; full-refresh the destination table."
                )
            event_time = self._cluster_time_to_datetime(event.get("clusterTime"))
            if event_time is not None and event_time > self._init_dt:
                break
            record = self._change_event_to_record(event, for_deletes)
            last_token = event.get("_id")
            if record is None:
                continue
            records.append(record)

        if last_token is not None:
            end_offset = {_RESUME_TOKEN_KEY: self._token_to_str(last_token)}
        else:
            end_offset = start_offset or {}
        if not records:
            return iter([]), end_offset
        return iter(records), end_offset

    def _change_event_to_record(self, event: dict, for_deletes: bool) -> Optional[dict]:
        """Convert one change event into an envelope row, or skip it."""
        event_time = self._cluster_time_to_datetime(event.get("clusterTime"))
        if event_time is None:
            raise ValueError("Change stream event is missing clusterTime")
        if for_deletes:
            key = event.get("documentKey") or {}
            if _ID_FIELD not in key:
                raise ValueError("Delete event is missing documentKey._id")
            return {
                _ID_FIELD: str(key[_ID_FIELD]),
                _DOCUMENT_FIELD: None,
                _EVENT_TIME_FIELD: event_time,
            }
        document = event.get("fullDocument")
        if not document:
            return None
        return self._document_to_record(document, event_time)

    def _document_to_record(self, document: dict, event_time: datetime) -> dict:
        """Convert a BSON document into an envelope record."""
        from bson import json_util

        if _ID_FIELD not in document:
            raise ValueError("Encountered a document without an '_id' field")
        # ``JSONOptions`` instances are not picklable, so the Relaxed options
        # are reached through the module at call time rather than bound to a
        # module-level constant that Spark would serialise with the connector.
        # The framework converts this Extended JSON string to VARIANT via
        # ``VariantVal.parseJson``.
        return {
            _ID_FIELD: str(document[_ID_FIELD]),
            _DOCUMENT_FIELD: json_util.dumps(document, json_options=json_util.RELAXED_JSON_OPTIONS),
            _EVENT_TIME_FIELD: event_time,
        }

    def _change_stream_start_timestamp(self):
        """BSON Timestamp for the first watch when no resume token exists."""
        from bson.timestamp import Timestamp

        dt = self._init_dt
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return Timestamp(int(dt.timestamp()), 1)

    @staticmethod
    def _cluster_time_to_datetime(value) -> Optional[datetime]:
        """Convert a change-event ``clusterTime`` to UTC datetime."""
        from bson.timestamp import Timestamp

        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, Timestamp):
            return datetime.fromtimestamp(value.time, tz=timezone.utc)
        return None

    @staticmethod
    def _token_to_str(token) -> str:
        """Serialise a resume token as Extended JSON."""
        from bson import json_util

        return json_util.dumps(token, json_options=json_util.RELAXED_JSON_OPTIONS)

    @staticmethod
    def _str_to_resume_token(value: str):
        """Parse a checkpointed resume token back to BSON."""
        from bson import json_util

        try:
            return json_util.loads(value, json_options=json_util.RELAXED_JSON_OPTIONS)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid change-stream resume token: {value!r}") from exc

    @staticmethod
    def _watch_failure(exc: Exception) -> ValueError:
        """Map a failed ``watch()`` to an actionable ValueError."""
        message = str(exc)
        if "replica set" in message.lower() or getattr(exc, "code", None) == 40573:
            return ValueError(
                "MongoDB change streams require a replica set or Atlas cluster. "
                f"The server rejected watch(): {exc}"
            )
        return ValueError(
            f"MongoDB change stream could not be opened: {exc}. "
            "If the resume token is stale, full-refresh the destination table."
        )

    @staticmethod
    def _reject_removed_options(table_options: dict[str, str]) -> None:
        """Fail loudly if a retired table option is still set."""
        present = [name for name in _REMOVED_TABLE_OPTIONS if table_options.get(name)]
        if not present:
            return
        raise ValueError(
            "MongoDB table options "
            + ", ".join(repr(name) for name in present)
            + " are no longer supported. Every collection is ingested via "
            "change streams into `_id`, `document` (VARIANT), and "
            "`event_time`. Remove the option(s) from table_configuration."
        )

    @staticmethod
    def _resolve_max_records(table_options: dict[str, str]) -> int:
        """Parse the optional ``max_records_per_batch`` table option."""
        raw = table_options.get("max_records_per_batch")
        if raw is None or str(raw).strip() == "":
            return _DEFAULT_MAX_RECORDS_PER_BATCH
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid 'max_records_per_batch' option: {raw!r}") from exc
        if value <= 0:
            raise ValueError(f"'max_records_per_batch' must be positive, got {value}")
        return value

    @staticmethod
    def _resolve_batch_size(table_options: dict[str, str]) -> int:
        """Parse the optional ``batch_size`` table option (0 = driver default)."""
        raw = table_options.get("batch_size")
        if raw is None or str(raw).strip() == "":
            return 0
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid 'batch_size' option: {raw!r}") from exc
        if value < 0:
            raise ValueError(f"'batch_size' must be non-negative, got {value}")
        return value

    def _validate_table(self, table_name: str) -> None:
        supported = self.list_tables()
        if table_name not in supported:
            raise ValueError(f"Collection '{table_name}' not found. Available: {supported}")
