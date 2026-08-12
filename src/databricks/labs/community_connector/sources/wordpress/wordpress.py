"""WordPressLakeflowConnect — Lakeflow Community Connector for the WordPress REST API.

Targets WordPress core (self-hosted, 5.6+) under ``/wp-json/wp/v2/`` with
Application Password (HTTP Basic) authentication.

Tables
------
    posts, pages, media   cdc      — cursor ``modified_gmt``   (partitioned)
    comments              append   — cursor ``date_gmt``        (partitioned)
    categories, tags      snapshot — no cursor                  (read_table)
    users                 snapshot — no cursor                  (read_table)
    taxonomies            snapshot — dict keyed by slug         (read_table)

Partitioned streaming strategy
------------------------------
``posts`` / ``pages`` / ``media`` / ``comments`` support server-side time-range
filters (``modified_after`` / ``modified_before`` and ``after`` / ``before``),
so they use the ``SupportsPartitionedStream`` path:

* ``latest_offset`` returns the high-water mark, capped at the connector's
  init time so ``Trigger.AvailableNow`` terminates (a fresh trigger gets a
  newer cap).  It is a lightweight, metadata-only call — no records are read.
* ``get_partitions`` splits the ``(start, end]`` cursor range into
  ``num_partitions`` contiguous time windows that Spark distributes across
  executors.
* ``read_partition`` fetches and paginates one window on an executor.

Windows are contiguous and disjoint: the lower bound is exclusive (``after``)
and the upper bound is made inclusive at second precision by querying
``before = until + 1s``.  This guarantees every record falls in exactly one
window — no gaps, no cross-partition duplicates.

Snapshot tables have no incremental filter, so they fall back to
``read_table`` via ``is_partitioned() == False``.
"""

from typing import Any, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.wordpress.wordpress_schemas import (
    SUPPORTED_TABLES,
    TABLE_CONFIG,
    TABLE_SCHEMAS,
    build_metadata,
)
from databricks.labs.community_connector.sources.wordpress.wordpress_utils import (
    DEFAULT_PER_PAGE,
    MAX_PER_PAGE,
    WordPressError,
    add_seconds,
    build_session,
    normalize_ts,
    now_utc_iso,
    paginate,
    parse_ts,
    request_with_retry,
)

# Lower bound used when neither a prior offset nor a user-supplied
# ``start_timestamp`` is available.  Wide enough to cover any real site; the
# fixed ``num_partitions`` split keeps the partition count bounded regardless.
DEFAULT_START_TIMESTAMP = "1970-01-01T00:00:00Z"
DEFAULT_NUM_PARTITIONS = 4
DEFAULT_LOOKBACK_SECONDS = 0

# Default sliding window applied per micro-batch (7 days — matches the repo's
# incremental-connector convention).  Bounds batch size when the stream is far
# behind (e.g. a long-paused pipeline resuming, or a user-supplied historical
# ``start_timestamp``): the offset advances one window at a time instead of
# jumping the whole span at once.  The initial backfill from the epoch default
# is deliberately exempt (see ``latest_offset``).  Set ``window_seconds=0`` to
# disable windowing entirely.
DEFAULT_WINDOW_SECONDS = 7 * 24 * 60 * 60

# When ``num_partitions`` is not set explicitly, the cursor range is split into
# partitions of at most this many seconds each (30 days) so a wide backfill fans
# out across more executor tasks instead of a few very large ones.
AUTO_PARTITION_TARGET_SECONDS = 30 * 24 * 60 * 60
# Ceiling on the auto-derived partition count so an extreme range (e.g. the
# epoch default) can't explode into thousands of tasks.
MAX_AUTO_PARTITIONS = 32


class WordPressLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """Lakeflow connector for the WordPress REST API (``wp/v2``)."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        base_url = options.get("base_url")
        username = options.get("username")
        application_password = options.get("application_password")

        if not base_url:
            raise ValueError("WordPress connector requires 'base_url' in options")
        if not username:
            raise ValueError("WordPress connector requires 'username' in options")
        if not application_password:
            raise ValueError("WordPress connector requires 'application_password' in options")

        # Site root, e.g. https://example.com — the wp/v2 path is appended per
        # request.  Trailing slashes / an accidental /wp-json suffix are trimmed.
        root = base_url.rstrip("/")
        if root.endswith("/wp-json"):
            root = root[: -len("/wp-json")]
        self._api_base = f"{root}/wp-json/wp/v2"

        self._username = username
        self._application_password = application_password
        self._session = build_session(username, application_password)

        # Freeze the upper bound at init time so latest_offset returns a stable
        # value across every micro-batch in a single Trigger.AvailableNow run.
        # Data modified after this instant is picked up by the next trigger,
        # which constructs a fresh connector with a newer init time.
        self._init_time = now_utc_iso()

    # ------------------------------------------------------------------ #
    # Interface: discovery / schema / metadata
    # ------------------------------------------------------------------ #

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return build_metadata(table_name)

    # ------------------------------------------------------------------ #
    # Interface: non-partitioned reads (snapshot tables via simpleStreamReader)
    # ------------------------------------------------------------------ #

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        cfg = TABLE_CONFIG[table_name]

        if cfg["partitioned"]:
            raise ValueError(
                f"Table '{table_name}' uses the partitioned streaming path; "
                "read_table is not used for it."
            )

        # Snapshot tables: full read, no checkpointable offset.
        if cfg.get("dict_shaped"):
            return iter(self._read_dict_shaped(table_name)), {}
        return iter(self._read_snapshot_list(table_name, table_options)), {}

    # ------------------------------------------------------------------ #
    # SupportsPartitionedStream
    # ------------------------------------------------------------------ #

    def is_partitioned(self, table_name: str) -> bool:
        self._validate_table(table_name)
        return bool(TABLE_CONFIG[table_name]["partitioned"])

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the high-water mark for the table, capped at init time.

        Metadata-only: no records are read here.  With ``window_seconds > 0``
        (default) the offset advances by one window per micro-batch, bounding
        batch size when the stream is far behind; otherwise it jumps straight to
        the init-time cap.  Either way the value stabilises once the stream
        catches up, so ``Trigger.AvailableNow`` terminates.

        The initial backfill from the epoch default (no prior offset **and** no
        user ``start_timestamp``) is exempt from windowing: splitting decades of
        empty pre-history into fixed windows would emit thousands of empty
        micro-batches.  It advances straight to the cap instead — records still
        stream lazily, split across ``num_partitions``.  Supply a
        ``start_timestamp`` to bound the backfill into windows too.
        """
        self._validate_table(table_name)
        window_seconds = self._int_option(table_options, "window_seconds", DEFAULT_WINDOW_SECONDS)
        current = (start_offset or {}).get("cursor")
        if not current:
            current = self._resolve_start(table_options)
        if window_seconds > 0 and current != DEFAULT_START_TIMESTAMP:
            next_end = add_seconds(current, window_seconds)
            return {"cursor": min(next_end, self._init_time)}
        return {"cursor": self._init_time}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> list[dict]:
        """Split the ``(start, end]`` cursor range into contiguous time windows.

        The number of windows is ``num_partitions`` when set explicitly,
        otherwise auto-scaled to the range width (see
        ``_resolve_num_partitions``): a wide backfill fans out across more
        executor tasks instead of a few very large ones.  All windows belong to
        the same micro-batch, so this does not affect termination.
        """
        self._validate_table(table_name)

        start_cursor = (start_offset or {}).get("cursor") if start_offset else None
        if not start_cursor:
            start_cursor = self._resolve_start(table_options)

        if end_offset is not None:
            end_cursor = end_offset.get("cursor") or self._init_time
        else:
            # Batch mode: cover the whole table up to the init-time cap.
            end_cursor = self._init_time

        # Apply the lookback only once, at the lower bound of the range, to
        # catch records edited during a prior window.  The stored cursor is
        # never widened by this.
        lookback = self._int_option(table_options, "lookback_seconds", DEFAULT_LOOKBACK_SECONDS)
        if lookback > 0 and start_cursor != DEFAULT_START_TIMESTAMP:
            start_cursor = add_seconds(start_cursor, -lookback)

        start_dt = parse_ts(start_cursor)
        end_dt = parse_ts(end_cursor)
        if start_dt is None or end_dt is None or start_dt >= end_dt:
            return []

        total_seconds = (end_dt - start_dt).total_seconds()
        num_partitions = self._resolve_num_partitions(table_options, total_seconds)
        step = total_seconds / num_partitions

        partitions: list[dict] = []
        for i in range(num_partitions):
            since = add_seconds(start_cursor, int(round(step * i)))
            if i == num_partitions - 1:
                until = end_cursor
            else:
                until = add_seconds(start_cursor, int(round(step * (i + 1))))
            if parse_ts(since) >= parse_ts(until):
                continue
            partitions.append({"since": since, "until": until})
        return partitions

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Fetch one time window on an executor.

        Self-contained: rebuilds its own session from ``self.options`` rather
        than relying on any driver-side state.
        """
        self._validate_table(table_name)
        cfg = TABLE_CONFIG[table_name]

        session = build_session(self._username, self._application_password)
        url = f"{self._api_base}/{cfg['endpoint']}"

        # Lower bound exclusive (``after``); upper bound made inclusive at
        # second precision via ``before = until + 1s`` so a record landing on
        # an interior boundary belongs to exactly one window.
        params: dict[str, Any] = {
            "orderby": cfg["sort_field"],
            "order": "asc",
        }
        params[cfg["after_param"]] = partition["since"]
        params[cfg["before_param"]] = add_seconds(partition["until"], 1)

        per_page = self._int_option(table_options, "per_page", DEFAULT_PER_PAGE)
        per_page = max(1, min(per_page, MAX_PER_PAGE))

        return self._emit(paginate(session, url, params, per_page=per_page))

    # ------------------------------------------------------------------ #
    # Snapshot readers
    # ------------------------------------------------------------------ #

    def _read_snapshot_list(self, table_name: str, table_options: dict[str, str]) -> Iterator[dict]:
        """Full page-through of a list endpoint (categories / tags / users)."""
        cfg = TABLE_CONFIG[table_name]
        url = f"{self._api_base}/{cfg['endpoint']}"
        per_page = self._int_option(table_options, "per_page", DEFAULT_PER_PAGE)
        per_page = max(1, min(per_page, MAX_PER_PAGE))
        yield from self._emit(paginate(self._session, url, {}, per_page=per_page))

    def _read_dict_shaped(self, table_name: str) -> Iterator[dict]:
        """Read a dict-keyed-by-slug metadata endpoint (``taxonomies``).

        WordPress returns ``{slug: {...}}``; the simulator serves the corpus as
        an array of already-flattened rows.  Both shapes are handled so live
        and simulate modes agree: dict values are promoted to rows with the
        dict key surfaced as ``slug``.
        """
        cfg = TABLE_CONFIG[table_name]
        url = f"{self._api_base}/{cfg['endpoint']}"
        response = request_with_retry(self._session, url, params=None)
        if response.status_code != 200:
            raise WordPressError(f"Failed to read '{table_name}': HTTP {response.status_code}")
        body = response.json()
        if isinstance(body, dict):
            for slug, value in body.items():
                if isinstance(value, dict):
                    row = dict(value)
                    row.setdefault("slug", slug)
                    yield row
        elif isinstance(body, list):
            yield from body
        else:
            raise WordPressError(
                f"Unexpected response shape for '{table_name}': {type(body).__name__}"
            )

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _emit(records: Iterator[dict]) -> Iterator[dict]:
        """Pass records through unchanged (raw parsed JSON for the framework)."""
        yield from records

    def _resolve_num_partitions(self, table_options: dict[str, str], total_seconds: float) -> int:
        """Partition count for the current range.

        An explicit ``num_partitions`` is honored as-is.  Otherwise the count is
        auto-scaled so each partition spans at most
        ``AUTO_PARTITION_TARGET_SECONDS``: a wide backfill fans out across more
        executor tasks (bounded by ``MAX_AUTO_PARTITIONS``) instead of a few very
        large ones, while a narrow incremental range stays at the
        ``DEFAULT_NUM_PARTITIONS`` floor.  Pure function of its inputs, so
        ``get_partitions`` stays deterministic across retries.
        """
        if table_options.get("num_partitions") is not None:
            return max(1, self._int_option(table_options, "num_partitions", DEFAULT_NUM_PARTITIONS))
        by_span = int(
            (total_seconds + AUTO_PARTITION_TARGET_SECONDS - 1) // AUTO_PARTITION_TARGET_SECONDS
        )
        return max(DEFAULT_NUM_PARTITIONS, min(MAX_AUTO_PARTITIONS, by_span))

    def _resolve_start(self, table_options: dict[str, str]) -> str:
        """Starting cursor for the first micro-batch of a partitioned table."""
        start = table_options.get("start_timestamp")
        normalized = normalize_ts(start) if start else None
        return normalized or DEFAULT_START_TIMESTAMP

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_CONFIG:
            raise ValueError(f"Unsupported table: {table_name!r}. Supported: {SUPPORTED_TABLES}")

    @staticmethod
    def _int_option(table_options: dict[str, str], key: str, default: int) -> int:
        raw = table_options.get(key)
        if raw is None:
            return default
        try:
            return int(raw)
        except (TypeError, ValueError):
            return default
