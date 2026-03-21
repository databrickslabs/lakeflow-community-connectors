"""Denodo Virtual DataPort connector via PostgreSQL-compatible protocol (psycopg2).

Denodo exposes a PostgreSQL-compatible interface on port 9996 (default).
All SQL queries are internally translated to VQL and executed against
virtualized data sources.
"""

from datetime import datetime, timezone
from typing import Dict, Iterator, List

import psycopg2
import psycopg2.extras
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface import LakeflowConnect

# ---------------------------------------------------------------------------
# Denodo/PostgreSQL → Spark type mapping
# ---------------------------------------------------------------------------
_TYPE_MAP = {
    "text": StringType(),
    "varchar": StringType(),
    "character varying": StringType(),
    "char": StringType(),
    "character": StringType(),
    "bpchar": StringType(),
    "name": StringType(),
    "uuid": StringType(),
    "json": StringType(),
    "jsonb": StringType(),
    "xml": StringType(),
    "int2": IntegerType(),
    "smallint": IntegerType(),
    "int4": IntegerType(),
    "integer": IntegerType(),
    "int": IntegerType(),
    "serial": IntegerType(),
    "int8": LongType(),
    "bigint": LongType(),
    "bigserial": LongType(),
    "float4": FloatType(),
    "real": FloatType(),
    "float8": DoubleType(),
    "double precision": DoubleType(),
    "numeric": DecimalType(38, 18),
    "decimal": DecimalType(38, 18),
    "bool": BooleanType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "timestamp without time zone": TimestampType(),
    "timestamptz": TimestampType(),
    "timestamp with time zone": TimestampType(),
    "bytea": BinaryType(),
    "oid": LongType(),
}


def _map_denodo_type(data_type: str, numeric_precision=None, numeric_scale=None):
    """Map a Denodo/PostgreSQL data_type string to a Spark DataType."""
    dt_lower = data_type.lower().strip()

    # Handle array types: Denodo reports "ARRAY" or "_typename"
    if dt_lower == "array" or dt_lower.startswith("_"):
        return ArrayType(StringType())

    # Handle numeric with explicit precision/scale
    if dt_lower in ("numeric", "decimal") and numeric_precision is not None:
        prec = int(numeric_precision)
        scale = int(numeric_scale) if numeric_scale is not None else 0
        return DecimalType(prec, scale)

    return _TYPE_MAP.get(dt_lower, StringType())


class DenodoLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Denodo Virtual DataPort."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._host = options["host"]
        self._port = int(options.get("port", "9996"))
        self._database = options["database"]
        self._user = options["user"]
        self._password = options["password"]
        self._ssl_mode = options.get("ssl_mode", "prefer")

        # Sync configuration
        self._sync_mode = options.get("sync_mode", "full")
        self._cursor_column = options.get("cursor_column", "")
        self._batch_size = int(options.get("batch_size", "10000"))

        # Filter configuration (comma-separated)
        self._schemas_filter = _parse_csv(options.get("schemas", ""))
        self._tables_filter = _parse_csv(options.get("tables", ""))
        self._exclude_tables = _parse_csv(options.get("exclude_tables", ""))

        # Per-table cursor column overrides: "schema.table=col,schema2.table2=col2"
        self._cursor_overrides: Dict[str, str] = {}
        for item in _parse_csv(options.get("cursor_overrides", "")):
            if "=" in item:
                tbl, col = item.split("=", 1)
                self._cursor_overrides[tbl.strip()] = col.strip()

        # Cap cursors at init time to avoid chasing new data within a trigger.
        self._init_time = datetime.now(timezone.utc).isoformat()

        # Establish connection
        self._conn = self._connect()

        # Caches
        self._tables_cache: List[str] | None = None
        self._pk_cache: Dict[str, List[str]] = {}

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _connect(self):
        conn = psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._database,
            user=self._user,
            password=self._password,
            sslmode=self._ssl_mode,
            application_name="lakeflow-denodo-connector",
        )
        conn.set_session(readonly=True, autocommit=True)
        return conn

    def _execute(self, query: str, params=None) -> list[dict]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            if cur.description:
                return [dict(row) for row in cur.fetchall()]
            return []

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        if self._tables_cache is not None:
            return self._tables_cache

        query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """
        params: list = []
        if self._schemas_filter:
            placeholders = ",".join(["%s"] * len(self._schemas_filter))
            query += f" AND table_schema IN ({placeholders})"
            params.extend(self._schemas_filter)

        query += " ORDER BY table_schema, table_name"
        rows = self._execute(query, params or None)

        tables: list[str] = []
        for row in rows:
            table_id = f"{row['table_schema']}.{row['table_name']}"
            if self._tables_filter and table_id not in self._tables_filter:
                continue
            if table_id in self._exclude_tables:
                continue
            tables.append(table_id)

        self._tables_cache = tables
        return tables

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        schema_name, tbl_name = _split_table(table_name)

        columns = self._get_columns(schema_name, tbl_name)
        fields: list[StructField] = []
        for col in columns:
            spark_type = _map_denodo_type(
                col["data_type"],
                col.get("numeric_precision"),
                col.get("numeric_scale"),
            )
            nullable = col.get("is_nullable", "YES") == "YES"
            fields.append(StructField(col["column_name"], spark_type, nullable=nullable))

        # Metadata columns
        fields.append(StructField("_denodo_vdb", StringType(), nullable=True))
        fields.append(StructField("_denodo_schema", StringType(), nullable=True))
        fields.append(StructField("_denodo_ingested_at", StringType(), nullable=True))

        return StructType(fields)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        schema_name, tbl_name = _split_table(table_name)

        # Discover primary keys
        pks = self._get_primary_keys(schema_name, tbl_name)

        # Determine cursor column for this table
        cursor_col = self._cursor_overrides.get(table_name, self._cursor_column)

        # Determine ingestion type
        if self._sync_mode == "incremental" and cursor_col:
            if pks:
                ingestion_type = "cdc"
            else:
                ingestion_type = "append"
        else:
            ingestion_type = "snapshot"

        metadata: dict = {"ingestion_type": ingestion_type}

        if pks:
            metadata["primary_keys"] = pks

        # For snapshot without PKs, use first column as synthetic PK
        if ingestion_type == "snapshot" and not pks:
            columns = self._get_columns(schema_name, tbl_name)
            if columns:
                metadata["primary_keys"] = [columns[0]["column_name"]]

        if ingestion_type in ("cdc", "append") and cursor_col:
            metadata["cursor_field"] = cursor_col

        return metadata

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        metadata = self.read_table_metadata(table_name, table_options)
        ingestion_type = metadata["ingestion_type"]

        if ingestion_type == "snapshot":
            return self._read_snapshot(table_name)

        cursor_col = metadata.get("cursor_field", "")
        return self._read_incremental(table_name, start_offset, cursor_col)

    # ------------------------------------------------------------------
    # Read strategies
    # ------------------------------------------------------------------

    def _read_snapshot(self, table_name: str) -> tuple[Iterator[dict], dict]:
        """Full-refresh read. Returns all rows with offset=None (non-checkpointable)."""
        schema_name, tbl_name = _split_table(table_name)
        query = f'SELECT * FROM "{schema_name}"."{tbl_name}"'
        rows = self._execute(query)
        enriched = [self._enrich(row, schema_name) for row in rows]
        return iter(enriched), None

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict,
        cursor_col: str,
    ) -> tuple[Iterator[dict], dict]:
        """Cursor-column-based incremental read."""
        schema_name, tbl_name = _split_table(table_name)
        since = start_offset.get("cursor") if start_offset else None

        # Already caught up to init time — no more data.
        if since and since >= self._init_time:
            return iter([]), start_offset

        query = f'SELECT * FROM "{schema_name}"."{tbl_name}"'
        params: list = []

        if since:
            query += f' WHERE "{cursor_col}" > %s'
            params.append(since)

        query += f' ORDER BY "{cursor_col}" ASC'

        if self._batch_size:
            query += " LIMIT %s"
            params.append(self._batch_size)

        rows = self._execute(query, params or None)

        if not rows:
            return iter([]), start_offset or {}

        enriched = [self._enrich(row, schema_name) for row in rows]

        # Last row has the max cursor value (rows are sorted ASC).
        last_cursor = rows[-1].get(cursor_col)
        if last_cursor is not None:
            # Normalize to string for JSON-serializable offset.
            last_cursor = str(last_cursor)

        end_offset = {"cursor": last_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(enriched), end_offset

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _enrich(self, row: dict, schema_name: str) -> dict:
        """Add Denodo metadata columns to a record."""
        row["_denodo_vdb"] = self._database
        row["_denodo_schema"] = schema_name
        row["_denodo_ingested_at"] = datetime.now(timezone.utc).isoformat()
        return row

    def _validate_table(self, table_name: str) -> None:
        tables = self.list_tables()
        if table_name not in tables:
            raise ValueError(f"Table '{table_name}' is not supported. Available tables: {tables}")

    def _get_columns(self, schema_name: str, tbl_name: str) -> list[dict]:
        query = """
            SELECT column_name, data_type, is_nullable,
                   character_maximum_length, numeric_precision,
                   numeric_scale, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        return self._execute(query, [schema_name, tbl_name])

    def _get_primary_keys(self, schema_name: str, tbl_name: str) -> list[str]:
        cache_key = f"{schema_name}.{tbl_name}"
        if cache_key in self._pk_cache:
            return self._pk_cache[cache_key]

        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
               ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """
        rows = self._execute(query, [schema_name, tbl_name])
        pks = [r["column_name"] for r in rows]
        self._pk_cache[cache_key] = pks
        return pks


# ---------------------------------------------------------------------------
# Module-level utilities
# ---------------------------------------------------------------------------


def _parse_csv(value: str) -> list[str]:
    """Split a comma-separated string into a trimmed list, dropping blanks."""
    return [s.strip() for s in value.split(",") if s.strip()] if value else []


def _split_table(table_name: str) -> tuple[str, str]:
    """Split 'schema.table' into (schema, table)."""
    parts = table_name.split(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Table name '{table_name}' must be in 'schema.table' format.")
    return parts[0], parts[1]
