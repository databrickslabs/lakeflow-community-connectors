# Denodo Virtual DataPort — Source API Documentation

## Overview

Denodo Virtual DataPort (VDP) 9.4 exposes a **PostgreSQL-compatible interface**
on port 9996 (default). The Lakeflow connector uses `psycopg2` to communicate
with Denodo as if it were a standard PostgreSQL database. Denodo translates all
incoming SQL into its native VQL and executes queries against virtualized sources
in real time.

## Connection

| Parameter        | Value / Description                              |
|------------------|--------------------------------------------------|
| Driver           | `psycopg2` (`pip install psycopg2-binary`)       |
| Host             | Configurable (Denodo VDP server)                 |
| Port             | 9996 (default for PostgreSQL interface)           |
| Database         | Virtual Database (VDB) name in Denodo            |
| User / Password  | Denodo user with read permissions                |
| SSL              | Optional (`sslmode=require` for encrypted)       |
| Connection String| `host={h} port={p} dbname={vdb} user={u} password={pw}` |

## Authentication

Standard PostgreSQL username/password authentication over the PostgreSQL
wire protocol. No OAuth or token-based auth — credentials are sent during
the `psycopg2.connect()` handshake.

## Metadata Discovery

### List Schemas (Denodo Folders)

```sql
SELECT DISTINCT table_schema
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
ORDER BY table_schema;
```

### List Views (Tables)

```sql
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
ORDER BY table_schema, table_name;
```

### Column Metadata

```sql
SELECT column_name, data_type, is_nullable,
       character_maximum_length, numeric_precision,
       numeric_scale, ordinal_position
FROM information_schema.columns
WHERE table_schema = '{schema}' AND table_name = '{table}'
ORDER BY ordinal_position;
```

### Primary Keys (Optional — Logical PKs in Denodo)

```sql
SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
   ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_schema = '{schema}'
  AND tc.table_name = '{table}'
  AND tc.constraint_type = 'PRIMARY KEY'
ORDER BY kcu.ordinal_position;
```

## Data Retrieval

### Full Load

```sql
SELECT * FROM "{schema}"."{table}";
```

### Incremental (Cursor-Column Based)

```sql
SELECT *
FROM "{schema}"."{table}"
WHERE "{cursor_column}" > '{last_cursor_value}'
ORDER BY "{cursor_column}" ASC
LIMIT {batch_size};
```

## Data Type Mapping

| Denodo / PostgreSQL Type         | Spark Type       |
|----------------------------------|------------------|
| text, varchar, char              | StringType       |
| int4, integer                    | IntegerType      |
| int8, bigint                     | LongType         |
| float4, real                     | FloatType        |
| float8, double precision         | DoubleType       |
| numeric, decimal                 | DecimalType      |
| bool, boolean                    | BooleanType      |
| date                             | DateType         |
| timestamp, timestamptz           | TimestampType    |
| bytea                            | BinaryType       |
| json, jsonb                      | StringType       |
| array                            | ArrayType(String)|

## Pagination / Rate Limits

Denodo does not expose a REST API with pagination — all data is fetched via SQL.
Use `LIMIT` / `OFFSET` or cursor-column filtering to control batch sizes.
There are no API rate limits; however, query execution time depends on the
underlying source systems.

## Known Limitations

1. **Not a real PostgreSQL** — Denodo emulates the PostgreSQL protocol.
   Features like `LISTEN/NOTIFY`, `COPY`, advisory locks, and PG-specific
   system catalogs (`pg_stat_*`, `pg_class`) are **not supported**.
2. **No native CDC** — Denodo is a virtualization layer and does not provide
   change data capture. Incremental sync relies on a user-defined cursor column.
3. **Query latency** — Every query is federated to underlying sources at
   runtime. Views with Full Cache enabled will be faster.
4. **View names with special characters** — Always quote identifiers with
   double quotes (`"schema"."view name"`).
5. **No physical primary keys** — Denodo views have optional *logical* PKs.
   The connector falls back to the first column if no PK is defined.
