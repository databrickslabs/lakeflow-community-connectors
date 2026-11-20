"""Custom handler for ``POST .../DataProvider/GetAdoDataSetForSqlCommand``.

OneStream's data API takes an arbitrary T-SQL string, so a declarative corpus
can't serve it. This handler executes the connector's (read-only, generated)
SQL against an in-memory SQLite database seeded from
``corpus/onestream_db.json``, after a small T-SQL -> SQLite translation:

* ``OFFSET n ROWS FETCH NEXT m ROWS ONLY`` -> ``LIMIT m OFFSET n``
* ``N'literal'`` -> ``'literal'``
* ``[dbo].[Table]`` and ``[INFORMATION_SCHEMA].[COLUMNS]`` work as-is (attached
  SQLite databases); the latter is generated from the corpus.
* The connector's ``sys.all_columns`` row-cap probe is answered directly.

Tests can mutate the database (``execute``), drop columns (``drop_column``) or
simulate a server-side row cap (``set_row_cap``); ``reset_database`` undoes it.

Like the real service, it returns ``{"<ResultDataTableName or Table>": [rows]}``,
``{"Table": []}`` for empty results, HTTP 400 for missing body fields and
HTTP 500 (with a OneStream-style ``Detail``) for SQL errors. Statements other
than a single SELECT are rejected so a connector bug can never look "fine".
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
from pathlib import Path

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import ResponseRecord
from databricks.labs.community_connector.source_simulator.interceptor import response_from_record

_CORPUS = Path(__file__).resolve().parent.parent / "corpus" / "onestream_db.json"
_OFFSET_FETCH = re.compile(
    r"OFFSET\s+(\d+)\s+ROWS\s+FETCH\s+NEXT\s+(\d+)\s+ROWS\s+ONLY", re.IGNORECASE
)
_NATIONAL = re.compile(r"\bN'")
_PROBE = re.compile(r"\[sys\]\.\[all_columns\]", re.IGNORECASE)
_LOCK = threading.Lock()
_CONN: sqlite3.Connection | None = None
_ROW_CAP: int | None = None


def _connection() -> sqlite3.Connection:
    global _CONN  # pylint: disable=global-statement
    with _LOCK:
        if _CONN is None:
            conn = sqlite3.connect(":memory:", check_same_thread=False)
            conn.execute("ATTACH DATABASE ':memory:' AS dbo")
            conn.execute("ATTACH DATABASE ':memory:' AS INFORMATION_SCHEMA")
            conn.execute(
                "CREATE TABLE INFORMATION_SCHEMA.COLUMNS "
                "(TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION)"
            )
            data = json.loads(_CORPUS.read_text(encoding="utf-8"))
            for table, rows in data.items():
                cols = list(rows[0].keys())
                conn.execute(
                    f"CREATE TABLE dbo.[{table}] (" + ", ".join(f"[{c}]" for c in cols) + ")"
                )
                conn.executemany(
                    f"INSERT INTO dbo.[{table}] VALUES (" + ", ".join("?" for _ in cols) + ")",
                    [tuple(r.get(c) for c in cols) for r in rows],
                )
                conn.executemany(
                    "INSERT INTO INFORMATION_SCHEMA.COLUMNS VALUES ('dbo', ?, ?, ?)",
                    [(table, c, i + 1) for i, c in enumerate(cols)],
                )
            conn.commit()
            _CONN = conn
        return _CONN


def _json(status: int, body, prep: PreparedRequest) -> Response:
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json; charset=utf-8"},
        body_text=json.dumps(body),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)


def _error(detail: str, prep: PreparedRequest) -> Response:
    return _json(
        500,
        {
            "Type": "https://tools.ietf.org/html/rfc9110#section-15.6.1",
            "Title": "An error occurred while processing your request.",
            "Status": 500,
            "Detail": detail,
        },
        prep,
    )


def translate(sql: str) -> str:
    sql = _NATIONAL.sub("'", sql)
    return _OFFSET_FETCH.sub(lambda m: f"LIMIT {m.group(2)} OFFSET {m.group(1)}", sql)


def serve_sql_command(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    raw = prep.body or b"{}"
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    body = json.loads(raw or "{}")
    missing = [f for f in ("BaseWebServerUrl", "ApplicationName", "SqlQuery") if not body.get(f)]
    if missing:
        return _json(
            400,
            {
                "errors": {f: [f"The {f} field is required."] for f in missing},
                "title": "One or more validation errors occurred.",
                "status": 400,
            },
            prep,
        )

    sql = body["SqlQuery"].strip()
    stripped = sql.rstrip(";")
    if ";" in stripped or not re.match(r"^\s*SELECT\b", stripped, re.IGNORECASE):
        return _error("Simulator only serves a single SELECT statement.", prep)
    if _PROBE.search(stripped):
        fetch = _OFFSET_FETCH.search(stripped)
        rows = [{"x": 1}] * (int(fetch.group(2)) if fetch else 0)
    else:
        try:
            cur = _connection().execute(translate(stripped))
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        except sqlite3.Error as exc:
            return _error(f"An error occurred executing the SQL query. {exc}", prep)
    if _ROW_CAP is not None:
        rows = rows[:_ROW_CAP]
    return _json(200, {body.get("ResultDataTableName") or "Table": rows}, prep)


def reset_database() -> None:
    """Discard in-test mutations; the next request reloads the corpus."""
    global _CONN, _ROW_CAP  # pylint: disable=global-statement
    with _LOCK:
        if _CONN is not None:
            _CONN.close()
        _CONN = None
        _ROW_CAP = None


def set_row_cap(cap: int | None) -> None:
    """Simulate a server that silently returns at most ``cap`` rows per request."""
    global _ROW_CAP  # pylint: disable=global-statement
    _ROW_CAP = cap


def drop_column(table: str, column: str) -> None:
    """Simulate an older OneStream version that lacks ``table.column``."""
    execute(f"ALTER TABLE dbo.[{table}] DROP COLUMN [{column}]")
    execute(
        "DELETE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?",
        (table, column),
    )


def execute(sql: str, params: tuple = ()) -> None:
    """Mutate the simulated application DB from a test (e.g. restamp a data unit)."""
    conn = _connection()
    conn.execute(sql, params)
    conn.commit()
