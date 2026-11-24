"""In-memory data store backing the simulated source.

Each table is registered with:
  - a schema (list of field descriptors)
  - metadata (primary keys, cursor field, ingestion type)
  - a primary key field name for identity

Records are plain dicts.  The store handles thread-safe CRUD, timestamp
management, and the "deleted records" sidecar for cdc_with_deletes tables.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


class TableDef:  # pylint: disable=too-few-public-methods
    """Holds the definition and live data for a single table."""

    __slots__ = (
        "name",
        "schema_fields",
        "metadata",
        "pk_field",
        "_records",
        "_deleted_records",
    )

    def __init__(
        self,
        name: str,
        schema_fields: list[dict],
        metadata: dict,
        pk_field: str,
    ) -> None:
        self.name = name
        self.schema_fields = schema_fields
        self.metadata = metadata
        self.pk_field = pk_field
        self._records: dict[str, dict] = {}
        self._deleted_records: list[dict] = []


class Store:
    """Thread-safe in-memory store for all tables."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tables: dict[str, TableDef] = {}

    # ── table registration ────────────────────────────────────────────

    def register_table(
        self,
        name: str,
        schema_fields: list[dict],
        metadata: dict,
        pk_field: str,
    ) -> None:
        """Register a new table with its schema, metadata, and primary key."""
        with self._lock:
            self._tables[name] = TableDef(name, schema_fields, metadata, pk_field)

    # ── table introspection ───────────────────────────────────────────

    def list_tables(self) -> list[str]:
        """Return the names of all registered tables."""
        with self._lock:
            return list(self._tables.keys())

    def get_table_schema(self, table_name: str) -> list[dict]:
        """Return the schema field descriptors for a table."""
        with self._lock:
            return list(self._get_table(table_name).schema_fields)

    def get_table_metadata(self, table_name: str) -> dict:
        """Return the metadata dict for a table."""
        with self._lock:
            return dict(self._get_table(table_name).metadata)

    def get_table_pk(self, table_name: str) -> str:
        """Return the primary key field name for a table."""
        with self._lock:
            return self._get_table(table_name).pk_field

    # ── record reads ──────────────────────────────────────────────────

    def list_records(  # pylint: disable=too-many-arguments
        self,
        table_name: str,
        *,
        since: Optional[str] = None,
        until: Optional[str] = None,
        cursor_field: Optional[str] = None,
        filters: Optional[dict[str, str]] = None,
        limit: Optional[int] = 100,
    ) -> list[dict]:
        """List records, optionally filtering by a cursor-field range.

        Args:
            table_name: Which table to query.
            since: Exclusive lower bound on ``cursor_field`` (records with
                   cursor value strictly greater than ``since``).
            until: Inclusive upper bound on ``cursor_field``.
            cursor_field: The field name to filter on.  If ``since`` or
                          ``until`` is given, this must be provided.
            filters: Optional exact-match filters (field_name → value).
            limit: Max records to return.  ``None`` means no limit.
        """
        with self._lock:
            tbl = self._get_table(table_name)
            records = list(tbl._records.values())

            if cursor_field and since is not None:
                records = [r for r in records if r.get(cursor_field, "") > since]
            if cursor_field and until is not None:
                records = [r for r in records if r.get(cursor_field, "") <= until]
            if filters:
                for field, value in filters.items():
                    records = [r for r in records if r.get(field) == value]

            sort_key = cursor_field or tbl.pk_field
            records.sort(key=lambda r: r.get(sort_key, ""))
            return records[:limit] if limit is not None else records

    def list_deleted_records(
        self,
        table_name: str,
        *,
        since: Optional[str] = None,
        cursor_field: Optional[str] = None,
        limit: Optional[int] = 100,
    ) -> list[dict]:
        """Return tombstone records for deleted rows, with optional cursor filtering."""
        with self._lock:
            tbl = self._get_table(table_name)
            records = list(tbl._deleted_records)

            if cursor_field and since is not None:
                records = [r for r in records if r.get(cursor_field, "") > since]

            sort_key = cursor_field or tbl.pk_field
            records.sort(key=lambda r: r.get(sort_key, ""))
            return records[:limit] if limit is not None else records

    def get_all_records(self, table_name: str) -> list[dict]:
        """Return all records for a table without any filtering."""
        with self._lock:
            tbl = self._get_table(table_name)
            return list(tbl._records.values())

    # ── record writes ─────────────────────────────────────────────────

    def insert_record(self, table_name: str, record: dict, ts_field: Optional[str] = None) -> dict:
        """Insert a new record, optionally setting a default timestamp field."""
        with self._lock:
            tbl = self._get_table(table_name)
            if ts_field:
                record.setdefault(ts_field, _iso(_now()))
            pk_val = record[tbl.pk_field]
            tbl._records[pk_val] = record
            return record

    def upsert_record(self, table_name: str, record: dict, ts_field: Optional[str] = None) -> dict:
        """Insert or update a record, advancing the timestamp field."""
        with self._lock:
            tbl = self._get_table(table_name)
            if ts_field:
                record[ts_field] = self._make_ts(tbl, ts_field)
            pk_val = record[tbl.pk_field]
            tbl._records[pk_val] = record
            return record

    def delete_record(
        self,
        table_name: str,
        pk_value: str,
        ts_field: Optional[str] = None,
        tombstone_fields: Optional[dict] = None,
    ) -> Optional[dict]:
        """Remove a record and append a tombstone to the deleted-records sidecar."""
        with self._lock:
            tbl = self._get_table(table_name)
            record = tbl._records.pop(pk_value, None)
            if record is None:
                return None

            tombstone = {tbl.pk_field: pk_value}
            if tombstone_fields:
                tombstone.update(tombstone_fields)
            if ts_field:
                tombstone[ts_field] = self._make_ts(tbl, ts_field)

            for field in tbl.schema_fields:
                fname = field["name"]
                if fname not in tombstone:
                    tombstone[fname] = None

            tbl._deleted_records.append(tombstone)
            return tombstone

    # ── internal ──────────────────────────────────────────────────────

    @staticmethod
    def _make_ts(tbl: TableDef, ts_field: str) -> str:
        """Return a timestamp string matching the declared type of *ts_field*.

        ``date`` fields get an ISO date (``YYYY-MM-DD``); everything else
        gets a full ISO timestamp so cursor comparisons remain consistent
        with the format used in seed data.
        """
        field_type = None
        for f in tbl.schema_fields:
            if f["name"] == ts_field:
                field_type = f.get("type")
                break
        now = _now()
        if field_type == "date":
            return now.date().isoformat()
        return _iso(now)

    def _get_table(self, table_name: str) -> TableDef:
        tbl = self._tables.get(table_name)
        if tbl is None:
            raise ValueError(f"Unknown table: {table_name}. Available: {list(self._tables.keys())}")
        return tbl

    # ── seeding helper ────────────────────────────────────────────────

    def seed_records(
        self,
        table_name: str,
        records: list[dict],
    ) -> None:
        """Bulk-load initial records into a table, keyed by primary key."""
        with self._lock:
            tbl = self._get_table(table_name)
            for rec in records:
                pk_val = rec[tbl.pk_field]
                tbl._records[pk_val] = rec
