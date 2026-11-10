"""The only place the OneStream connector builds SQL.

``GetAdoDataSetForSqlCommand`` executes whatever T-SQL it receives with the
PAT owner's (Administrator) rights, including DML and multi-statement batches.
The builders here therefore accept only:

* identifiers matching ``[A-Za-z_][A-Za-z0-9_]*`` (always bracket-quoted), and
* Python ``int`` literals.

No free-text value is ever interpolated. Member *names* supplied by users are
resolved to integer ids in Python before any data query is built.
"""

from __future__ import annotations

import re
from typing import Iterable, Sequence

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

SQL_MIN_YEAR = 1900
SQL_MAX_YEAR = 2100


def sql_ident(name: str) -> str:
    """Bracket-quote a validated identifier."""
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return f"[{name}]"


def sql_table_ref(table: str, schema: str = "dbo") -> str:
    return f"{sql_ident(schema)}.{sql_ident(table)}"


def sql_int_lit(value) -> str:
    """Render an integer literal; rejects bools, floats and strings."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Expected int literal, got {value!r}")
    return str(value)


def sql_int_list(values: Iterable[int]) -> str:
    items = [sql_int_lit(v) for v in values]
    if not items:
        raise ValueError("IN list must not be empty")
    return ", ".join(items)


def sql_data_record_table(year: int) -> str:
    year_lit = int(sql_int_lit(year))
    if not SQL_MIN_YEAR <= year_lit <= SQL_MAX_YEAR:
        raise ValueError(f"Year out of range: {year}")
    return sql_table_ref(f"DataRecord{year_lit}")


def sql_conj(*predicates: str | None) -> str:
    parts = [p for p in predicates if p]
    return " AND ".join(parts) if parts else "1 = 1"


def sql_eq(col: str, value: int) -> str:
    return f"{sql_ident(col)} = {sql_int_lit(value)}"


def sql_in(col: str, values: Sequence[int] | None) -> str | None:
    """``col IN (...)``; ``None`` (no filter) when values is None."""
    if values is None:
        return None
    return f"{sql_ident(col)} IN ({sql_int_list(values)})"


def sql_gt(col: str, value: int) -> str:
    return f"{sql_ident(col)} > {sql_int_lit(value)}"


def sql_le(col: str, value: int) -> str:
    return f"{sql_ident(col)} <= {sql_int_lit(value)}"


def sql_pair_filter(col_a: str, col_b: str, pairs: Sequence[Sequence[int]]) -> str:
    """``((a = x AND b = y) OR ...)`` for a set of integer pairs."""
    if not pairs:
        raise ValueError("pair filter must not be empty")
    terms = [f"({sql_eq(col_a, a)} AND {sql_eq(col_b, b)})" for a, b in pairs]
    return "(" + " OR ".join(terms) + ")"


def sql_select(
    columns: Sequence[str],
    source: str,
    where: str = "1 = 1",
    order_by: Sequence[str] | None = None,
    offset: int | None = None,
    limit: int | None = None,
    distinct: bool = False,
) -> str:
    """Build a single SELECT. Paging uses ``OFFSET .. FETCH`` (requires ORDER BY)."""
    cols = ", ".join(sql_ident(c) for c in columns)
    sql = f"SELECT {'DISTINCT ' if distinct else ''}{cols} FROM {source} WHERE {where}"
    if order_by:
        sql += " ORDER BY " + ", ".join(sql_ident(c) for c in order_by)
    if limit is not None:
        if not order_by:
            raise ValueError("Paging requires ORDER BY")
        sql += f" OFFSET {sql_int_lit(offset or 0)} ROWS FETCH NEXT {sql_int_lit(limit)} ROWS ONLY"
    return sql


def sql_group_max(group_columns: Sequence[str], value_column: str, source: str, where: str) -> str:
    """``SELECT <groups>, MAX(value) AS [max_value] ... GROUP BY <groups>``."""
    groups = ", ".join(sql_ident(c) for c in group_columns)
    return (
        f"SELECT {groups}, MAX({sql_ident(value_column)}) AS [max_value] "
        f"FROM {source} WHERE {where} GROUP BY {groups}"
    )


def sql_eq_name(col: str, name: str) -> str:
    """``col = N'name'`` where ``name`` must itself be identifier-shaped.

    Only used for catalog lookups (``INFORMATION_SCHEMA``); the regex check
    rules out quotes, so the literal cannot be escaped.
    """
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(f"Invalid identifier literal: {name!r}")
    return f"{sql_ident(col)} = N'{name}'"


def sql_row_probe(n: int) -> str:
    """Return exactly ``n`` one-column rows, to detect a server-side row cap."""
    return (
        "SELECT 1 AS [x] FROM [sys].[all_columns] AS [a] CROSS JOIN [sys].[all_columns] AS [b] "
        f"ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT {sql_int_lit(n)} ROWS ONLY"
    )
