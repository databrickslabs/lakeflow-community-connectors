import json
import time
from itertools import islice
from unittest.mock import MagicMock, patch

import pytest
import requests

from databricks.labs.community_connector.sources.palantir.palantir import PalantirLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestPalantirConnector(LakeflowConnectTests):
    connector_class = PalantirLakeflowConnect
    simulator_source = "palantir"
    replay_config = {
        "token": "simulator-fake-token",
        "hostname": "simulator.palantirfoundry.com",
        "ontology_api_name": "ontology-simulator",
    }

    def _tables(self):
        """Restrict shared-suite iteration to FlightsFinal.

        The connector itself still exposes every object type in the
        ontology via ``list_tables()`` (and ``test_list_tables``
        verifies that contract). Parameterised tests like
        ``test_read_table``, ``test_read_terminates``, etc. only
        exercise FlightsFinal because:

        - The simulator's single ``corpus/objects.json`` serves every
          ``/loadObjects`` query, so iterating the full live ontology
          floods the validator with per-record additive drift across
          unrelated object types (aircraft fields show up in flight
          responses and vice versa).
        - Live tests against the production ontology drain millions
          of rows across multiple tables. FlightsFinal alone covers
          every code path the parameterised tests assert on — it
          carries ``arrivalTimestamp`` (needed by the
          ``synthesize_future_records`` cap exercise) and is the
          single object type the simulator's
          ``object_types_list.json`` declares.

        Custom tests (``test_search_endpoint_coverage``,
        ``test_where_clause_built_for_incremental_call``) pin
        ``table = "FlightsFinal"`` explicitly and only assert
        ``list_tables()`` contains it — they don't iterate other
        object types. In live mode the ontology has many ``Example*``
        types whose response shapes drift from the FlightsFinal
        corpus; pinning keeps the validator clean.
        """
        return ["FlightsFinal"]

    def test_where_clause_built_for_incremental_call(self):
        """When ``start_offset`` carries a ``max_cursor_value`` older
        than the value returned by the ``search`` endpoint, the
        connector's incremental path must build a server-side
        ``where: gt`` filter on ``cursor_field`` and pass it to
        ``loadObjects`` via ``_build_object_set``.

        ``test_read_terminates`` does not exercise this branch in
        simulate mode because the static simulate corpus returns the
        same max-cursor value every call, so
        ``new_max_cursor == prev`` and ``_read_incremental``
        early-exits before reaching the where-clause builder. This
        test forces the branch by handing in a deliberately stale
        offset (1970) so the search result is strictly greater and
        the early-exit short-circuit does **not** fire — the read
        falls through to ``loadObjects`` with the ``where: gt`` filter.
        """
        # Pin to ``FlightsFinal`` — the canonical anchor table for
        # this connector's tests. Iterating ``list_tables()`` and
        # picking the first match in live mode lands on other object
        # types (``ExampleFlight`` etc.) whose responses don't match
        # the simulator's FlightsFinal corpus, so the live validator
        # flags spurious drift even though the connector code path
        # is identical.
        table = "FlightsFinal"
        cursor_field = "arrivalTimestamp"
        assert table in self.connector.list_tables(), (
            f"Ontology must expose '{table}' for this test."
        )
        assert cursor_field in (
            self.connector.get_table_schema(table, {}).fieldNames()
        ), f"'{table}' must carry '{cursor_field}'."
        # Cap reads — in live mode the table can be millions of rows;
        # we only need _fetch_page invoked once to verify the
        # where:gt argument shape.
        options = {
            "cursor_field": cursor_field,
            "page_size": "100",
            "max_records_per_batch": "5",
        }
        stale_offset = {"max_cursor_value": "1970-01-01T00:00:00Z"}

        original = self.connector._fetch_page
        with patch.object(
            self.connector, "_fetch_page", wraps=original
        ) as fetch_page_spy:
            records, _ = self.connector.read_table(
                table, stale_offset, options
            )
            list(records)  # drain (capped) generator so _fetch_page is called

        assert fetch_page_spy.called, "_fetch_page was not invoked"
        # First call's object_set should be the filter wrapper.
        first_call = fetch_page_spy.call_args_list[0]
        object_set = first_call.args[0]
        assert object_set["type"] == "filter", (
            f"Expected filter object_set with where:gt clause, got: {object_set}"
        )
        where = object_set["where"]
        assert where["type"] == "gt"
        assert where["field"] == cursor_field
        assert where["value"] == "1970-01-01T00:00:00Z"

    def test_search_endpoint_coverage(self):
        """Directly exercise the search endpoint so live record runs
        register a hit on it.

        ``search`` is the connector's max-cursor lookup endpoint and
        gets hit on every CDC read via ``_get_max_cursor_value``.
        This test issues a direct call so coverage is complete even
        if the read path is mocked out by other tests in the suite.

        Pinned to ``FlightsFinal`` + ``arrivalTimestamp`` so the
        live validator compares the response against the matching
        FlightsFinal corpus rather than landing on an alphabetically
        earlier ``Example*`` object type whose schema differs.
        """
        table = "FlightsFinal"
        cursor_field = "arrivalTimestamp"
        assert table in self.connector.list_tables(), (
            f"Ontology must expose '{table}' for this test."
        )
        # Returns None or a value; either is acceptable — we only care
        # that POST /objects/{table}/search was issued.
        self.connector._get_max_cursor_via_search(table, cursor_field)

    def test_snapshot_read_through_simulator(self):
        """F-TEST-3: exercise the snapshot read path end-to-end through the
        simulator (real ``_fetch_page`` -> ``loadObjects`` -> response
        parsing), not a mock. ``FlightsFinal`` with no ``cursor_field``
        routes to ``_read_snapshot``. Bounded via ``islice`` so a
        live/record run doesn't drain the full table."""
        table = "FlightsFinal"
        records_iter, offset = self.connector.read_table(
            table, {}, {"page_size": "100"}
        )
        records = list(islice(records_iter, 50))
        assert records, "snapshot read should yield records from the corpus"
        assert offset == {}, "snapshot returns an empty offset (no resume)"
        # Records parse through the real loadObjects path; declared
        # (non-system) fields are a subset of the discovered schema.
        schema_fields = set(
            self.connector.get_table_schema(table, {}).fieldNames()
        )
        declared = {k for k in records[0] if not k.startswith("__")}
        assert declared <= schema_fields, (
            f"snapshot record fields not in schema: {declared - schema_fields}"
        )


class TestPalantirMaxCursorLookup:
    """Unit-level coverage for the max-cursor lookup path. The
    connector delegates ``_get_max_cursor_value`` straight to
    ``_get_max_cursor_via_search`` — the earlier aggregate-first
    path was removed because Palantir's aggregate endpoint returns
    500 for the ontology object types we use, making the search
    fallback the only path that ever returned a useful value.
    """

    @staticmethod
    def _connector() -> PalantirLakeflowConnect:
        return PalantirLakeflowConnect({
            "token": "fake",
            "hostname": "fake.palantirfoundry.com",
            "ontology_api_name": "ontology-fake",
        })

    def test_value_returned_from_search(self):
        c = self._connector()
        with patch.object(
            c, "_get_max_cursor_via_search", return_value="2026-01-01"
        ) as srch:
            result = c._get_max_cursor_value("FlightsFinal", "date")
        assert result == "2026-01-01"
        srch.assert_called_once_with("FlightsFinal", "date")

    def test_none_returned_when_search_fails(self):
        """Search returning None propagates through (caller treats it
        as ``no new data`` and the offset-unchanged termination check
        kicks in)."""
        c = self._connector()
        with patch.object(
            c, "_get_max_cursor_via_search", return_value=None
        ) as srch:
            result = c._get_max_cursor_value("FlightsFinal", "date")
        assert result is None
        srch.assert_called_once_with("FlightsFinal", "date")


class TestPalantirCursorTypes:
    """Cap and merge logic must handle non-timestamp cursor types
    (numeric IDs, UUIDs) without crashing. Comparing ``int > str``
    raises TypeError in Python 3, so the connector skips the
    ``_init_time`` cap for non-timestamp cursors.
    """

    @staticmethod
    def _connector() -> PalantirLakeflowConnect:
        c = PalantirLakeflowConnect({
            "token": "fake",
            "hostname": "fake.palantirfoundry.com",
            "ontology_api_name": "ontology-fake",
        })
        # Pre-seed the object-type cache so read_table doesn't try to
        # hit the (unreachable) live API to discover the schema.
        c._object_types_cache = {
            "FlightsFinal": {
                "primaryKey": "row_id",
                "properties": {},
            }
        }
        return c

    def test_to_utc_datetime_parses_known_palantir_formats(self):
        """``_to_utc_datetime`` must normalise every ISO 8601 form
        Palantir is known to return into a tz-aware UTC datetime so
        the cap comparison is correct regardless of the source's
        format/timezone choice."""
        from datetime import datetime as _dt, timezone as _tz
        c = self._connector()

        # Z suffix → UTC.
        assert c._to_utc_datetime("2026-05-09T13:30:00Z") == _dt(
            2026, 5, 9, 13, 30, tzinfo=_tz.utc
        )
        # Fractional seconds + Z.
        assert c._to_utc_datetime("2026-05-09T13:30:00.500Z") == _dt(
            2026, 5, 9, 13, 30, 0, 500_000, tzinfo=_tz.utc
        )
        # Explicit UTC offset.
        assert c._to_utc_datetime("2026-05-09T13:30:00+00:00") == _dt(
            2026, 5, 9, 13, 30, tzinfo=_tz.utc
        )
        # Non-UTC offset converts to UTC (EST = UTC-5).
        assert c._to_utc_datetime("2026-05-09T08:30:00-05:00") == _dt(
            2026, 5, 9, 13, 30, tzinfo=_tz.utc
        )
        # Naive string is assumed UTC.
        assert c._to_utc_datetime("2026-05-09T13:30:00") == _dt(
            2026, 5, 9, 13, 30, tzinfo=_tz.utc
        )
        # Date-only parses as midnight UTC.
        assert c._to_utc_datetime("2026-05-09") == _dt(
            2026, 5, 9, 0, 0, tzinfo=_tz.utc
        )
        # Space separator + microseconds — the exact form Palantir
        # returns for ``arrivalTimestamp`` (the primary cursor field).
        # ``fromisoformat`` has accepted the space separator since 3.10,
        # so the cap stays engaged (no silent lexicographic fallback).
        assert c._to_utc_datetime("2026-04-19 06:20:35.746137") == _dt(
            2026, 4, 19, 6, 20, 35, 746137, tzinfo=_tz.utc
        )
        # Space separator + Z suffix normalises like the T forms.
        assert c._to_utc_datetime("2026-04-19 06:20:35Z") == _dt(
            2026, 4, 19, 6, 20, 35, tzinfo=_tz.utc
        )
        # Non-strings and unparseable values return None so the cap
        # branch silently skips for non-timestamp cursors.
        assert c._to_utc_datetime(12345) is None
        assert c._to_utc_datetime(None) is None
        assert c._to_utc_datetime("flight_id_123") is None
        assert c._to_utc_datetime("550e8400-e29b-41d4-a716-446655440000") is None

    def test_cap_filters_records_past_init_time_in_non_utc_tz(self):
        """A non-UTC cursor that resolves to a UTC time *past*
        ``_init_time`` must be filtered out even though the raw
        string compares lexicographically less than ``_init_time``'s
        Z form. Strategy B applies the cap per-record during
        iteration, so the past-init record never gets emitted."""
        c = self._connector()
        # Pin both the ISO form (kept for backward-compatible
        # introspection) and the pre-parsed datetime the cap loop
        # actually reads.
        c._init_time = "2026-05-09T13:30:00Z"
        c._init_dt = c._to_utc_datetime(c._init_time)
        # First record: 2026-05-09T08:00:00-05:00 == 13:00 UTC (pre-init).
        # Second record: 2026-05-09T09:00:00-05:00 == 14:00 UTC (post-init).
        # Records arrive sorted ASC, so the second one breaks the
        # cap loop — only the first is emitted.
        records = [
            {"row_id": "a", "arrivalTimestamp": "2026-05-09T08:00:00-05:00"},
            {"row_id": "b", "arrivalTimestamp": "2026-05-09T09:00:00-05:00"},
        ]
        with patch.object(c, "_fetch_page", return_value=(records, None)):
            emitted_iter, offset = c.read_table(
                "FlightsFinal", None, {"cursor_field": "arrivalTimestamp"}
            )
            emitted = list(emitted_iter)
        assert len(emitted) == 1
        assert emitted[0]["row_id"] == "a"
        # Lex compare on raw strings would have left the EST form
        # past-init in, breaking the cap. Datetime-aware compare
        # correctly identifies the post-init record.
        assert offset == {
            "max_cursor_value": "2026-05-09T08:00:00-05:00",
            "max_tiebreak_value": "a",
        }

    def test_numeric_cursor_does_not_crash_on_cap(self):
        """When ``cursor_field`` is an int (e.g. auto-incrementing ID),
        the cap's datetime comparison would raise ``TypeError`` if not
        gated. ``_to_utc_datetime`` returns ``None`` for non-strings,
        so the cap branch silently skips and all records flow through.
        Offset = last numeric cursor."""
        c = self._connector()
        records = [{"row_id": str(i), "seq": 100 + i} for i in range(3)]
        with patch.object(c, "_fetch_page", return_value=(records, None)):
            emitted_iter, offset = c.read_table(
                "FlightsFinal", None, {"cursor_field": "seq"}
            )
            emitted = list(emitted_iter)
        assert len(emitted) == 3
        assert offset == {"max_cursor_value": 102, "max_tiebreak_value": "2"}

    def test_date_cursor_caps_at_init_time_boundary(self):
        """When cursor_field is a date (10-char ``YYYY-MM-DD``), the
        per-record cap correctly stops at the first record dated
        after ``_init_time``. The offset is the last pre-init date
        — a 10-char string, matching the cursor's shape."""
        c = self._connector()
        c._init_time = "2026-05-09T13:30:00Z"
        c._init_dt = c._to_utc_datetime(c._init_time)
        # Records sorted ASC by date — last two are past init.
        records = [
            {"row_id": "a", "date": "2026-05-07"},
            {"row_id": "b", "date": "2026-05-08"},
            {"row_id": "c", "date": "2026-05-10"},   # post-init: filtered
            {"row_id": "d", "date": "2026-05-11"},   # post-init: filtered
        ]
        with patch.object(c, "_fetch_page", return_value=(records, None)):
            emitted_iter, offset = c.read_table(
                "FlightsFinal", None, {"cursor_field": "date"}
            )
            emitted = list(emitted_iter)
        # Only pre-init records emitted; offset is last pre-init cursor
        # (preserving the date-only shape).
        assert [r["row_id"] for r in emitted] == ["a", "b"]
        assert offset == {"max_cursor_value": "2026-05-08", "max_tiebreak_value": "b"}

    def test_early_exit_when_search_max_below_prev_cursor(self):
        """Early-exit short-circuit fires when the search peek returns a
        value strictly BELOW ``prev_max_cursor`` (nothing new exists).
        With a composite (cursor, tiebreaker) cursor the connector must
        NOT short-circuit on an *equal* max — un-read rows may still
        share that boundary cursor value — so the skip condition is
        strictly-less. ``loadObjects`` must not be called and the offset
        must round-trip unchanged so Spark Streaming terminates.
        """
        c = self._connector()
        prev_offset = {
            "max_cursor_value": "2026-04-02T00:00:00Z",
            "max_tiebreak_value": "z",
        }
        below = "2026-04-01T00:00:00Z"
        with patch.object(
            c, "_get_max_cursor_value", return_value=below
        ) as search_spy, patch.object(
            c, "_fetch_page"
        ) as fetch_spy:
            emitted_iter, offset = c.read_table(
                "FlightsFinal",
                prev_offset,
                {"cursor_field": "arrivalTimestamp"},
            )
            emitted = list(emitted_iter)
        assert emitted == []
        assert offset == prev_offset
        search_spy.assert_called_once_with("FlightsFinal", "arrivalTimestamp")
        fetch_spy.assert_not_called()

    def test_no_early_exit_when_search_max_greater(self):
        """When the search peek returns a value strictly greater than
        ``prev_max_cursor``, the short-circuit must NOT fire — the
        connector proceeds to ``loadObjects`` with the ``where: gt``
        filter so new records are picked up.
        """
        c = self._connector()
        prev = "2026-04-02T00:00:00Z"
        new_max = "2026-04-05T00:00:00Z"
        records = [
            {"row_id": "x", "arrivalTimestamp": "2026-04-03T00:00:00Z"},
        ]
        with patch.object(
            c, "_get_max_cursor_value", return_value=new_max
        ), patch.object(
            c, "_fetch_page", return_value=(records, None)
        ) as fetch_spy:
            emitted_iter, offset = c.read_table(
                "FlightsFinal",
                {"max_cursor_value": prev},
                {"cursor_field": "arrivalTimestamp"},
            )
            emitted = list(emitted_iter)
        assert [r["row_id"] for r in emitted] == ["x"]
        # Resuming from a cursor-only offset (no tiebreaker persisted),
        # the connector uses the legacy strict gt filter, and the new
        # offset now carries the tiebreaker for subsequent composite runs.
        assert offset == {
            "max_cursor_value": "2026-04-03T00:00:00Z",
            "max_tiebreak_value": "x",
        }
        # First call's object_set must be the where:gt filter.
        first_call = fetch_spy.call_args_list[0]
        object_set = first_call.args[0]
        assert object_set["type"] == "filter"
        assert object_set["where"] == {
            "type": "gt",
            "field": "arrivalTimestamp",
            "value": prev,
        }

    def test_no_early_exit_when_search_returns_none(self):
        """When ``_get_max_cursor_value`` returns ``None`` (search
        endpoint 4xx/5xx or unsupported for this table), the
        short-circuit must fall through — the existing ``where: gt``
        path still works without the search peek.
        """
        c = self._connector()
        prev = "2026-04-02T00:00:00Z"
        records = [
            {"row_id": "y", "arrivalTimestamp": "2026-04-03T00:00:00Z"},
        ]
        with patch.object(
            c, "_get_max_cursor_value", return_value=None
        ), patch.object(
            c, "_fetch_page", return_value=(records, None)
        ) as fetch_spy:
            emitted_iter, _ = c.read_table(
                "FlightsFinal",
                {"max_cursor_value": prev},
                {"cursor_field": "arrivalTimestamp"},
            )
            emitted = list(emitted_iter)
        assert [r["row_id"] for r in emitted] == ["y"]
        fetch_spy.assert_called()

    def test_first_run_skips_search_peek(self):
        """On the first run (``start_offset`` is ``None`` / empty),
        the connector must do a full incremental load and must not
        issue the search peek — there is no checkpoint to compare
        against, so the short-circuit would be meaningless.
        """
        c = self._connector()
        records = [
            {"row_id": "a", "arrivalTimestamp": "2026-04-01T00:00:00Z"},
        ]
        with patch.object(
            c, "_get_max_cursor_value"
        ) as search_spy, patch.object(
            c, "_fetch_page", return_value=(records, None)
        ):
            c.read_table(
                "FlightsFinal", None, {"cursor_field": "arrivalTimestamp"}
            )
            c.read_table(
                "FlightsFinal", {}, {"cursor_field": "arrivalTimestamp"}
            )
        search_spy.assert_not_called()

    def test_early_exit_handles_numeric_cursor(self):
        """The cursor comparison helper must work for numeric cursors
        too: when the search peek returns an int strictly below ``prev``,
        the short-circuit fires. ``_to_utc_datetime`` returns ``None``
        for non-strings, so the helper falls back to direct ``>``.
        """
        c = self._connector()
        prev_offset = {"max_cursor_value": 100, "max_tiebreak_value": "z"}
        with patch.object(
            c, "_get_max_cursor_value", return_value=99
        ), patch.object(c, "_fetch_page") as fetch_spy:
            emitted_iter, offset = c.read_table(
                "FlightsFinal",
                prev_offset,
                {"cursor_field": "seq"},
            )
            assert list(emitted_iter) == []
        assert offset == prev_offset
        fetch_spy.assert_not_called()

    def test_offset_advances_to_last_emitted_record(self):
        """Core Strategy B invariant: the offset is the cursor of the
        LAST emitted record, not the dataset max. This prevents the
        cap-at-max bug where records past ``max_records_per_batch``
        would be skipped (offset advances past unread records).

        With ``max_records_per_batch=2`` and 4 records available,
        only the first 2 are emitted and offset = cursor of the 2nd.
        The next microbatch will fetch records 3 and 4 via
        ``where: gt cursor_of_record_2``."""
        c = self._connector()
        records = [
            {"row_id": "a", "arrivalTimestamp": "2026-04-01T00:00:00Z"},
            {"row_id": "b", "arrivalTimestamp": "2026-04-02T00:00:00Z"},
            {"row_id": "c", "arrivalTimestamp": "2026-04-03T00:00:00Z"},
            {"row_id": "d", "arrivalTimestamp": "2026-04-04T00:00:00Z"},
        ]
        with patch.object(c, "_fetch_page", return_value=(records, None)):
            emitted_iter, offset = c.read_table(
                "FlightsFinal",
                None,
                {
                    "cursor_field": "arrivalTimestamp",
                    "max_records_per_batch": "2",
                },
            )
            emitted = list(emitted_iter)
        assert [r["row_id"] for r in emitted] == ["a", "b"]
        # Offset now carries the tiebreaker (row_id) alongside the
        # cursor so the next microbatch resumes with the composite
        # (cursor, tiebreaker) filter — no tie-group can be split.
        assert offset == {
            "max_cursor_value": "2026-04-02T00:00:00Z",
            "max_tiebreak_value": "b",
        }

    def test_tied_cursor_at_cap_boundary_not_lost(self):
        """Regression for the CDC tied-cursor data-loss bug (F1): when
        records sharing one cursor value straddle the ``max_records``
        boundary, the composite (cursor, tiebreaker) offset must resume
        WITHIN the tie-group (``gt`` on the tiebreaker), so the un-emitted
        tied rows are recovered rather than skipped by a strict ``gt``
        on the cursor alone.
        """
        c = self._connector()
        # Three records share one timestamp; cap=2 splits the tie group.
        tied = [
            {"row_id": "a", "arrivalTimestamp": "2026-04-01T00:00:00Z"},
            {"row_id": "b", "arrivalTimestamp": "2026-04-01T00:00:00Z"},
            {"row_id": "c", "arrivalTimestamp": "2026-04-01T00:00:00Z"},
        ]
        topts = {"cursor_field": "arrivalTimestamp", "max_records_per_batch": "2"}

        # Batch 1: first two of the tie group, offset carries (ts, row_id).
        with patch.object(c, "_fetch_page", return_value=(tied, None)):
            it1, offset1 = c.read_table("FlightsFinal", None, topts)
            emitted1 = list(it1)
        assert [r["row_id"] for r in emitted1] == ["a", "b"]
        assert offset1 == {
            "max_cursor_value": "2026-04-01T00:00:00Z",
            "max_tiebreak_value": "b",
        }

        # Batch 2: dataset max cursor EQUALS the checkpoint, but the
        # un-read tied row 'c' must still be fetched (no early-exit), via
        # the composite where clause. Simulate the filtered page = [c].
        with patch.object(
            c, "_get_max_cursor_value", return_value="2026-04-01T00:00:00Z"
        ), patch.object(
            c, "_fetch_page", return_value=([tied[2]], None)
        ) as fetch_spy:
            it2, offset2 = c.read_table("FlightsFinal", offset1, topts)
            emitted2 = list(it2)
        # The tied remainder is recovered, NOT silently dropped.
        assert [r["row_id"] for r in emitted2] == ["c"]
        fetch_spy.assert_called()
        # And the resume filter is the composite (cursor, tiebreaker).
        object_set = fetch_spy.call_args_list[0].args[0]
        assert object_set["type"] == "filter"
        assert object_set["where"] == {
            "type": "or",
            "value": [
                {
                    "type": "gt",
                    "field": "arrivalTimestamp",
                    "value": "2026-04-01T00:00:00Z",
                },
                {
                    "type": "and",
                    "value": [
                        {
                            "type": "eq",
                            "field": "arrivalTimestamp",
                            "value": "2026-04-01T00:00:00Z",
                        },
                        {"type": "gt", "field": "row_id", "value": "b"},
                    ],
                },
            ],
        }


class TestPalantirSessionCleanup:
    """``requests.Session`` holds a keep-alive connection pool;
    Spark's data-source lifecycle constructs many connector instances
    per query so each leaked pool costs sockets on long-running
    pipelines. ``close()`` releases the pool.
    """

    @staticmethod
    def _connector() -> PalantirLakeflowConnect:
        return PalantirLakeflowConnect({
            "token": "fake",
            "hostname": "fake.palantirfoundry.com",
            "ontology_api_name": "ontology-fake",
        })

    def test_close_releases_session(self):
        c = self._connector()
        session = c._session
        with patch.object(session, "close") as session_close:
            c.close()
        session_close.assert_called_once()
        assert c._session is None, (
            "close() must null out _session so accidental post-close "
            "API calls fail loudly instead of using a closed pool."
        )

    def test_close_is_idempotent(self):
        c = self._connector()
        c.close()
        c.close()  # second call must not raise


class TestPalantirSnapshotRead:
    """Snapshot mode (no ``cursor_field`` in table_options) exercises
    the ``_read_snapshot`` path. The shared-suite parameterised tests
    (``test_read_table``, ``test_read_terminates``,
    ``test_every_column_populated_by_at_least_one_record``) run against
    ``dev_table_config.json``'s FlightsFinal entry, which is CDC-mode
    (cursor_field=arrivalTimestamp). Without these unit tests, the
    snapshot path would not be covered by CI at all.

    Three behaviors verified:
      1. Records returned by ``_fetch_page`` flow through unchanged.
      2. Offset is ``{}`` — the M1 invariant that lets the framework's
         equality-based termination kick in after the first call.
      3. The objectSet passed to ``_fetch_page`` is the base shape
         (no ``where`` filter), distinguishing snapshot from CDC's
         ``where: gt`` path.
    """

    @staticmethod
    def _connector() -> PalantirLakeflowConnect:
        c = PalantirLakeflowConnect({
            "token": "fake",
            "hostname": "fake.palantirfoundry.com",
            "ontology_api_name": "ontology-fake",
        })
        # Pre-seed the object-type cache so read_table doesn't try to
        # hit the (unreachable) live API to discover the schema.
        c._object_types_cache = {
            "FlightsFinal": {
                "primaryKey": "flightId",
                "properties": {
                    "flightId": {"dataType": {"type": "string"}},
                    "carrierCode": {"dataType": {"type": "string"}},
                },
            }
        }
        return c

    def test_snapshot_emits_records(self):
        c = self._connector()
        records = [
            {"flightId": "a", "carrierCode": "AA"},
            {"flightId": "b", "carrierCode": "BB"},
        ]
        with patch.object(c, "_fetch_page", return_value=(records, None)):
            emitted_iter, _ = c.read_table("FlightsFinal", None, {})
            emitted = list(emitted_iter)
        assert [r["flightId"] for r in emitted] == ["a", "b"]

    def test_snapshot_returns_empty_offset_for_one_call_termination(self):
        """Snapshot mode must return ``{}`` so the framework's
        equality-based termination fires after one call — no full
        re-pagination per trigger (the M1 fix)."""
        c = self._connector()
        with patch.object(c, "_fetch_page", return_value=([], None)):
            _, offset = c.read_table("FlightsFinal", None, {})
        assert offset == {}, (
            "Snapshot must return {} so framework terminates after one "
            "call. Returning anything else (e.g. {'done':'true'}) "
            "triggers a full re-pagination per trigger."
        )

    def test_snapshot_emits_all_records_ignoring_max_records_per_batch(self):
        """Snapshot mode must emit every record from the source even
        when ``max_records_per_batch`` is set small. Snapshot reads
        run in a single framework-driven pass with no mid-snapshot
        checkpoint, so capping would silently drop records past the
        cap — a data-loss bug. ``max_records_per_batch`` applies only
        to incremental mode where the ``last_emitted_cursor`` offset
        enables resume on the next microbatch."""
        c = self._connector()
        records = [{"flightId": f"r{i}", "carrierCode": "AA"} for i in range(10)]
        with patch.object(c, "_fetch_page", return_value=(records, None)):
            emitted_iter, _ = c.read_table(
                "FlightsFinal", None,
                {"page_size": "100", "max_records_per_batch": "3"},
            )
            emitted = list(emitted_iter)
        assert len(emitted) == 10, (
            f"Snapshot truncated by max_records_per_batch: emitted "
            f"{len(emitted)} of 10. Snapshot mode must stream all "
            f"records — the cap option applies to incremental only."
        )

    def test_snapshot_uses_base_object_set_no_filter(self):
        """Snapshot mode passes a base objectSet (no ``where`` filter)
        to ``_fetch_page`` — distinct from CDC's filtered ``where: gt``
        path. Catches accidental filter leakage from the incremental
        code path."""
        c = self._connector()
        with patch.object(c, "_fetch_page", return_value=([], None)) as fetch_spy:
            list(c.read_table("FlightsFinal", None, {})[0])
        first_call = fetch_spy.call_args_list[0]
        object_set = first_call.args[0]
        assert object_set == {"type": "base", "objectType": "FlightsFinal"}, (
            f"Expected base objectSet, got: {object_set}"
        )


class TestPalantirHostnameNormalization:
    """``hostname`` option must accept either the documented bare form
    (``yourcompany.palantirfoundry.com``) or a full URL pasted from the
    browser (``https://yourcompany.palantirfoundry.com/``) — silently
    normalising both into a clean ``self.base_url``. Mirrors the
    framework convention used in sources/osipi/osipi_http.py.
    """

    @staticmethod
    def _make(hostname: str) -> PalantirLakeflowConnect:
        return PalantirLakeflowConnect({
            "token": "fake",
            "hostname": hostname,
            "ontology_api_name": "ontology-fake",
        })

    def test_bare_hostname_gets_https_scheme(self):
        c = self._make("yourcompany.palantirfoundry.com")
        assert c.base_url == "https://yourcompany.palantirfoundry.com"

    def test_https_scheme_preserved_no_double_prefix(self):
        # The motivating case from the review nit — must NOT become
        # ``https://https://...``.
        c = self._make("https://yourcompany.palantirfoundry.com")
        assert c.base_url == "https://yourcompany.palantirfoundry.com"

    def test_http_scheme_rejected(self):
        # F9: a plaintext http:// base URL would send the bearer token
        # unencrypted, so it is rejected rather than used. Foundry is
        # HTTPS-only and the docs specify https:// exclusively.
        with pytest.raises(ValueError, match="https://"):
            self._make("http://yourcompany.palantirfoundry.com")

    def test_trailing_slash_stripped(self):
        c = self._make("https://yourcompany.palantirfoundry.com/")
        assert c.base_url == "https://yourcompany.palantirfoundry.com"

    def test_whitespace_stripped(self):
        c = self._make("  yourcompany.palantirfoundry.com  ")
        assert c.base_url == "https://yourcompany.palantirfoundry.com"

    def test_empty_hostname_raises(self):
        with pytest.raises(ValueError, match="hostname"):
            self._make("")

    def test_whitespace_only_hostname_raises(self):
        with pytest.raises(ValueError, match="hostname"):
            self._make("   ")


class TestPalantirDecimalMapping:
    """``decimal`` properties must preserve precision/scale via Spark's
    DecimalType rather than coerce to DoubleType (silent precision
    loss past ~15 digits is unacceptable for finance / measurement
    ontologies).
    """

    @staticmethod
    def _connector() -> PalantirLakeflowConnect:
        return PalantirLakeflowConnect({
            "token": "fake",
            "hostname": "fake.palantirfoundry.com",
            "ontology_api_name": "ontology-fake",
        })

    def test_decimal_uses_palantir_precision_and_scale(self):
        from pyspark.sql.types import DecimalType
        c = self._connector()
        spark_type = c._map_palantir_type_to_spark(
            {"type": "decimal", "precision": 12, "scale": 4}
        )
        assert spark_type == DecimalType(12, 4)

    def test_decimal_defaults_when_precision_missing(self):
        from pyspark.sql.types import DecimalType
        c = self._connector()
        # No precision / scale on the type definition — fall back to
        # (38, 18), the conventional wide default.
        assert c._map_palantir_type_to_spark({"type": "decimal"}) == DecimalType(38, 18)

    def test_decimal_precision_clamped_to_spark_max(self):
        from pyspark.sql.types import DecimalType
        c = self._connector()
        # Spark caps precision at 38; Palantir reporting a wider type
        # must clamp rather than raise.
        spark_type = c._map_palantir_type_to_spark(
            {"type": "decimal", "precision": 50, "scale": 10}
        )
        assert spark_type == DecimalType(38, 10)


def _mock_response(status: int, json_payload: dict = None, headers: dict = None):
    """Build a ``requests.Response``-like mock for ``_fetch_page``."""
    r = MagicMock()
    r.status_code = status
    # Default to an empty dict so ``response.headers.get("...")`` returns
    # ``None`` (the MagicMock auto-attribute would otherwise return a
    # nested MagicMock, breaking Retry-After parsing in tests).
    r.headers = headers or {}
    r.json.return_value = json_payload or {}
    # ``response.text`` carries Palantir's structured error body, which
    # the connector folds into the RuntimeError it raises on a
    # non-transient status (F3).
    r.text = json.dumps(json_payload) if json_payload is not None else ""
    if status >= 400:
        r.raise_for_status.side_effect = requests.HTTPError(
            f"HTTP {status}", response=r
        )
    else:
        r.raise_for_status.return_value = None
    return r


class TestPalantirFetchPageRetry:
    """``_fetch_page`` must retry only on transient failures
    (429, 503, ConnectionError, Timeout). Real 4xx errors like 401
    (expired token) or 404 (wrong ontology) must fail fast — otherwise
    the user waits 1+2+4+8 = 15 s for an unrecoverable error.
    """

    @staticmethod
    def _connector() -> PalantirLakeflowConnect:
        return PalantirLakeflowConnect({
            "token": "fake",
            "hostname": "fake.palantirfoundry.com",
            "ontology_api_name": "ontology-fake",
        })

    def test_401_fails_fast(self):
        """A 401 (expired token) must propagate on the first attempt —
        no sleep, no retry. Verifies the session is hit exactly once
        and ``time.sleep`` is not called from the retry path.
        """
        c = self._connector()
        with patch.object(
            c._session, "post", return_value=_mock_response(401)
        ) as post_spy, patch.object(time, "sleep") as sleep_spy:
            with pytest.raises(RuntimeError, match="401"):
                c._fetch_page({"type": "base", "objectType": "T"})
        assert post_spy.call_count == 1
        sleep_spy.assert_not_called()

    def test_404_fails_fast(self):
        """A 404 (wrong ontology name / unknown object type) must
        propagate on the first attempt rather than retrying through
        the misconfiguration.
        """
        c = self._connector()
        with patch.object(
            c._session, "post", return_value=_mock_response(404)
        ) as post_spy, patch.object(time, "sleep") as sleep_spy:
            with pytest.raises(RuntimeError, match="404"):
                c._fetch_page({"type": "base", "objectType": "T"})
        assert post_spy.call_count == 1
        sleep_spy.assert_not_called()

    def test_non_transient_error_includes_palantir_error_body(self):
        """F3: a non-transient 4xx must raise a RuntimeError that folds
        in Palantir's structured error body (errorCode/errorName) and the
        status, so failures are actionable instead of a bare status line.
        """
        c = self._connector()
        err = {
            "errorCode": "INVALID_ARGUMENT",
            "errorName": "PropertiesNotFound",
            "parameters": {"properties": ["bogusField"]},
        }
        with patch.object(
            c._session, "post", return_value=_mock_response(400, err)
        ), patch.object(time, "sleep"):
            with pytest.raises(RuntimeError) as exc:
                c._fetch_page({"type": "base", "objectType": "T"})
        msg = str(exc.value)
        assert "400" in msg
        assert "PropertiesNotFound" in msg  # the error body is included

    def test_429_retries_then_succeeds(self):
        """A 429 followed by a 200 must succeed — the retry path is
        the whole point of the loop, so a transient rate-limit
        recovers without surfacing an error.
        """
        c = self._connector()
        responses = [
            _mock_response(429),
            _mock_response(200, {"data": [{"id": 1}], "nextPageToken": None}),
        ]
        with patch.object(
            c._session, "post", side_effect=responses
        ) as post_spy, patch.object(time, "sleep"):
            records, token = c._fetch_page(
                {"type": "base", "objectType": "T"}
            )
        assert records == [{"id": 1}]
        assert token is None
        assert post_spy.call_count == 2

    def test_503_retries_then_succeeds(self):
        """Same as 429 but for 503 (service unavailable)."""
        c = self._connector()
        responses = [
            _mock_response(503),
            _mock_response(503),
            _mock_response(200, {"data": [], "nextPageToken": None}),
        ]
        with patch.object(
            c._session, "post", side_effect=responses
        ) as post_spy, patch.object(time, "sleep"):
            c._fetch_page({"type": "base", "objectType": "T"})
        assert post_spy.call_count == 3

    def test_429_exhausted_raises_runtime_error(self):
        """When 429s persist past ``max_retries``, the connector must
        raise a clear ``RuntimeError`` carrying the last status — not
        swallow the failure or loop forever.
        """
        c = self._connector()
        with patch.object(
            c._session, "post", return_value=_mock_response(429)
        ) as post_spy, patch.object(time, "sleep"):
            with pytest.raises(RuntimeError, match="429"):
                c._fetch_page({"type": "base", "objectType": "T"})
        assert post_spy.call_count == 5

    def test_connection_error_retries(self):
        """A ``ConnectionError`` is transient (network blip) and must
        be retried up to ``max_retries`` — the user's pipeline
        shouldn't fail on a single dropped TCP connection.
        """
        c = self._connector()
        side_effects = [
            requests.ConnectionError("dropped"),
            requests.ConnectionError("dropped"),
            _mock_response(200, {"data": [], "nextPageToken": None}),
        ]
        with patch.object(
            c._session, "post", side_effect=side_effects
        ) as post_spy, patch.object(time, "sleep"):
            c._fetch_page({"type": "base", "objectType": "T"})
        assert post_spy.call_count == 3

    def test_connection_error_exhausted_raises_runtime_error(self):
        """When the network never recovers, the helper must raise a
        ``RuntimeError`` (not a bare ``ConnectionError``) so the
        caller gets a stable exception surface, and the original
        cause is preserved via ``__cause__`` for diagnostics.
        """
        c = self._connector()
        with patch.object(
            c._session,
            "post",
            side_effect=requests.ConnectionError("down"),
        ) as post_spy, patch.object(time, "sleep"):
            with pytest.raises(RuntimeError, match="network error") as exc:
                c._fetch_page({"type": "base", "objectType": "T"})
        assert post_spy.call_count == 5
        assert isinstance(exc.value.__cause__, requests.ConnectionError)

    def test_retry_backoff_uses_random_jitter(self):
        """Each retry sleep must include ``random.random()`` jitter so
        concurrent Spark tasks don't unblock in lockstep against
        Palantir's per-user rate cap.
        """
        c = self._connector()
        with patch.object(
            c._session, "post", side_effect=[
                _mock_response(429),
                _mock_response(200, {"data": [], "nextPageToken": None}),
            ]
        ), patch.object(time, "sleep"), patch(
            "databricks.labs.community_connector.sources.palantir.palantir.random.random",
            return_value=0.5,
        ) as rng:
            c._fetch_page({"type": "base", "objectType": "T"})
        assert rng.called, (
            "Retry backoff must call random.random() for jitter."
        )

    def test_429_honours_retry_after_seconds_header(self):
        """When the server returns ``Retry-After: <seconds>``, the
        connector honours it as the base wait instead of falling back
        to exponential backoff. Jitter is still applied on top.
        """
        c = self._connector()
        retry_response = _mock_response(429, headers={"Retry-After": "7"})
        success_response = _mock_response(
            200, {"data": [], "nextPageToken": None}
        )
        sleeps: list = []
        with patch.object(
            c._session, "post",
            side_effect=[retry_response, success_response],
        ), patch.object(time, "sleep", side_effect=lambda s: sleeps.append(s)):
            c._fetch_page({"type": "base", "objectType": "T"})
        assert len(sleeps) == 1
        # Retry-After=7 with jitter [0.5, 1.5) → sleep in [3.5, 10.5).
        # Default exponential would have been 2**0 = 1s ± jitter → up
        # to 1.5s, so any value >= 3.5 proves the header was honoured.
        assert 3.5 <= sleeps[0] < 10.5, (
            f"Expected Retry-After=7 honoured with jitter; got {sleeps[0]}"
        )

    def test_503_honours_retry_after_seconds_header(self):
        """Same Retry-After contract for 503 as for 429."""
        c = self._connector()
        retry_response = _mock_response(503, headers={"Retry-After": "10"})
        success_response = _mock_response(
            200, {"data": [], "nextPageToken": None}
        )
        sleeps: list = []
        with patch.object(
            c._session, "post",
            side_effect=[retry_response, success_response],
        ), patch.object(time, "sleep", side_effect=lambda s: sleeps.append(s)):
            c._fetch_page({"type": "base", "objectType": "T"})
        assert 5.0 <= sleeps[0] < 15.0

    def test_retry_falls_back_to_exp_backoff_without_retry_after(self):
        """No ``Retry-After`` header → exponential backoff path
        (with jitter) — the default behavior."""
        c = self._connector()
        sleeps: list = []
        with patch.object(
            c._session, "post", side_effect=[
                _mock_response(429),  # no headers → no Retry-After
                _mock_response(200, {"data": [], "nextPageToken": None}),
            ]
        ), patch.object(time, "sleep", side_effect=lambda s: sleeps.append(s)):
            c._fetch_page({"type": "base", "objectType": "T"})
        assert len(sleeps) == 1
        # attempt=0 → base 2**0 = 1s, jittered to [0.5, 1.5).
        assert 0.5 <= sleeps[0] < 1.5, (
            f"Expected exp backoff at attempt=0 with jitter; got {sleeps[0]}"
        )

    def test_unparseable_retry_after_falls_back_to_exp_backoff(self):
        """Malformed ``Retry-After`` (e.g. HTTP-date form we don't
        parse, or garbage) must not crash — fall back to exponential
        backoff."""
        c = self._connector()
        retry_response = _mock_response(
            429, headers={"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}
        )
        success_response = _mock_response(
            200, {"data": [], "nextPageToken": None}
        )
        sleeps: list = []
        with patch.object(
            c._session, "post",
            side_effect=[retry_response, success_response],
        ), patch.object(time, "sleep", side_effect=lambda s: sleeps.append(s)):
            c._fetch_page({"type": "base", "objectType": "T"})
        # Should fall back to attempt=0 exp backoff: [0.5, 1.5).
        assert 0.5 <= sleeps[0] < 1.5


class TestPalantirObjectTypesRetry:
    """F7: the schema-discovery GET (`_ensure_object_types_cached`) is the
    first request of every trigger, so it must retry transient 429/503 /
    network errors like the data path — and fail fast on other 4xx/5xx.
    """

    @staticmethod
    def _connector() -> PalantirLakeflowConnect:
        return PalantirLakeflowConnect({
            "token": "fake",
            "hostname": "fake.palantirfoundry.com",
            "ontology_api_name": "ontology-fake",
        })

    def test_objecttypes_get_retries_on_429_then_succeeds(self):
        c = self._connector()
        responses = [
            _mock_response(429),
            _mock_response(200, {"data": [{"apiName": "FlightsFinal"}]}),
        ]
        with patch.object(
            c._session, "get", side_effect=responses
        ) as get_spy, patch.object(time, "sleep"):
            tables = c.list_tables()
        assert get_spy.call_count == 2
        assert "FlightsFinal" in tables

    def test_objecttypes_get_retries_on_503_then_succeeds(self):
        c = self._connector()
        responses = [
            _mock_response(503),
            _mock_response(503),
            _mock_response(200, {"data": [{"apiName": "FlightsFinal"}]}),
        ]
        with patch.object(
            c._session, "get", side_effect=responses
        ) as get_spy, patch.object(time, "sleep"):
            c.list_tables()
        assert get_spy.call_count == 3

    def test_objecttypes_get_fails_fast_on_404(self):
        c = self._connector()
        err = {"errorCode": "NOT_FOUND", "errorName": "OntologyNotFound"}
        with patch.object(
            c._session, "get", return_value=_mock_response(404, err)
        ) as get_spy, patch.object(time, "sleep") as sleep_spy:
            with pytest.raises(RuntimeError, match="OntologyNotFound"):
                c.list_tables()
        assert get_spy.call_count == 1
        sleep_spy.assert_not_called()

    def test_objecttypes_get_exhausts_429_then_raises(self):
        c = self._connector()
        with patch.object(
            c._session, "get", return_value=_mock_response(429)
        ) as get_spy, patch.object(time, "sleep"):
            with pytest.raises(RuntimeError, match="429"):
                c.list_tables()
        assert get_spy.call_count == 5


class TestPalantirSchemaEdgeCases:
    """F2: schema discovery must fail clearly for property-less object
    types and must not silently swallow unmapped Palantir types."""

    @staticmethod
    def _connector() -> PalantirLakeflowConnect:
        return PalantirLakeflowConnect({
            "token": "fake",
            "hostname": "fake.palantirfoundry.com",
            "ontology_api_name": "ontology-fake",
        })

    def test_empty_object_type_raises_clear_error(self):
        """No properties and no primaryKey -> empty schema. Must raise an
        actionable ValueError rather than return StructType([]) (which
        Spark later rejects opaquely)."""
        c = self._connector()
        with pytest.raises(ValueError, match="no readable columns"):
            c._build_schema_from_object_type(
                {"apiName": "EmptyType", "properties": {}}
            )

    def test_unmapped_type_falls_back_to_string_with_warning(self):
        """An unrecognised Palantir type maps to StringType, but must log
        a warning so the silent stringification is visible."""
        from databricks.labs.community_connector.sources.palantir import (
            palantir as _pmod,
        )
        c = self._connector()
        with patch.object(_pmod.logger, "warning") as warn_spy:
            mapped = c._map_palantir_type_to_spark({"type": "mediaReference"})
        from pyspark.sql.types import StringType
        assert isinstance(mapped, StringType)
        warn_spy.assert_called_once()
        assert "mediaReference" in str(warn_spy.call_args)

    def test_known_type_does_not_warn(self):
        """A recognised type maps directly with no warning."""
        from databricks.labs.community_connector.sources.palantir import (
            palantir as _pmod,
        )
        from pyspark.sql.types import LongType
        c = self._connector()
        with patch.object(_pmod.logger, "warning") as warn_spy:
            mapped = c._map_palantir_type_to_spark({"type": "integer"})
        assert isinstance(mapped, LongType)
        warn_spy.assert_not_called()


class TestPalantirPagination:
    """F10/F11: exercise multi-page pagination (nextPageToken threading)
    and the snapshot read path through the real _fetch_page → loadObjects
    request. The single-page simulator default and the _fetch_page-mocking
    snapshot unit tests leave both wires uncovered.
    """

    @staticmethod
    def _connector() -> PalantirLakeflowConnect:
        c = PalantirLakeflowConnect({
            "token": "fake",
            "hostname": "fake.palantirfoundry.com",
            "ontology_api_name": "ontology-fake",
        })
        # Pre-seed the object-type cache so read_table_metadata doesn't
        # hit the (unreachable) live API.
        c._object_types_cache = {
            "FlightsFinal": {"primaryKey": "flightId", "properties": {}}
        }
        return c

    def test_pagination_threads_page_token_across_pages(self):
        """F10: _generate_all_pages must follow nextPageToken to the next
        page, and _fetch_page must send the previous page's token as
        ``pageToken``. Page 1 returns a token, page 2 returns null."""
        c = self._connector()
        page1 = _mock_response(
            200, {"data": [{"flightId": "1"}, {"flightId": "2"}],
                  "nextPageToken": "TOK1"}
        )
        page2 = _mock_response(
            200, {"data": [{"flightId": "3"}], "nextPageToken": None}
        )
        with patch.object(
            c._session, "post", side_effect=[page1, page2]
        ) as post_spy, patch.object(time, "sleep"):
            records = list(c._generate_all_pages("FlightsFinal", 100))
        # Records from BOTH pages, in order.
        assert [r["flightId"] for r in records] == ["1", "2", "3"]
        assert post_spy.call_count == 2
        # Page 1 request carries no pageToken; page 2 carries page 1's token.
        first_body = post_spy.call_args_list[0].kwargs["json"]
        second_body = post_spy.call_args_list[1].kwargs["json"]
        assert "pageToken" not in first_body
        assert second_body["pageToken"] == "TOK1"

    def test_snapshot_reads_through_loadobjects_without_orderby(self):
        """F11: snapshot mode (no cursor_field) must traverse
        _fetch_page → loadObjects and parse the response. The posted body
        must be a base objectSet with no ``orderBy`` and no ``where``
        filter, and the offset must be empty (one-pass, no resume)."""
        c = self._connector()
        page = _mock_response(
            200, {"data": [{"flightId": "a"}, {"flightId": "b"}],
                  "nextPageToken": None}
        )
        with patch.object(
            c._session, "post", side_effect=[page]
        ) as post_spy, patch.object(time, "sleep"):
            emitted_iter, offset = c.read_table("FlightsFinal", None, {})
            records = list(emitted_iter)
        assert [r["flightId"] for r in records] == ["a", "b"]
        assert offset == {}
        body = post_spy.call_args_list[0].kwargs["json"]
        assert "orderBy" not in body
        assert body["objectSet"] == {
            "type": "base", "objectType": "FlightsFinal"
        }


class TestPalantirTiebreakResolution:
    """F2: incremental reads must REFUSE (not silently fall back to a lossy
    strict-gt cursor) when no unique single-column tiebreaker resolves —
    i.e. a multi-column primary key or no declared primary key, with no
    explicit ``tiebreaker_field``. Also covers ``_resolve_tiebreak_field``
    branch selection (TEST-2 gap)."""

    @staticmethod
    def _connector(primary_key) -> PalantirLakeflowConnect:
        c = PalantirLakeflowConnect({
            "token": "fake",
            "hostname": "fake.palantirfoundry.com",
            "ontology_api_name": "ontology-fake",
        })
        c._object_types_cache = {"T": {"primaryKey": primary_key, "properties": {}}}
        return c

    def test_resolve_tiebreak_field_branches(self):
        # Explicit option wins over the PK.
        assert self._connector("pk1")._resolve_tiebreak_field(
            "T", {"tiebreaker_field": "explicit"}
        ) == "explicit"
        # Single-string PK.
        assert self._connector("pk1")._resolve_tiebreak_field("T", {}) == "pk1"
        # Single-element list PK.
        assert self._connector(["only"])._resolve_tiebreak_field("T", {}) == "only"
        # Multi-column PK -> None (no single sortable tiebreaker).
        assert self._connector(["a", "b"])._resolve_tiebreak_field("T", {}) is None
        # No PK -> None.
        assert self._connector(None)._resolve_tiebreak_field("T", {}) is None

    def test_incremental_refuses_on_multicolumn_pk(self):
        """Composite PK -> no single tiebreaker -> refuse rather than
        silently strict-gt (which would drop tied-cursor rows)."""
        c = self._connector(["a", "b"])
        with pytest.raises(ValueError, match="tiebreaker"):
            c.read_table("T", None, {"cursor_field": "ts"})

    def test_incremental_refuses_on_no_primary_key(self):
        c = self._connector(None)
        with pytest.raises(ValueError, match="tiebreaker"):
            c.read_table("T", None, {"cursor_field": "ts"})

    def test_explicit_tiebreaker_field_enables_cdc_on_multicolumn_pk(self):
        """An explicit tiebreaker_field lets a composite-PK table still do
        CDC — the refuse only fires when nothing resolves."""
        c = self._connector(["a", "b"])
        recs = [{"ts": "2026-01-01T00:00:00Z", "tb": "x"}]
        with patch.object(c, "_fetch_page", return_value=(recs, None)):
            emitted_iter, offset = c.read_table(
                "T", None, {"cursor_field": "ts", "tiebreaker_field": "tb"}
            )
            emitted = list(emitted_iter)
        assert [r["ts"] for r in emitted] == ["2026-01-01T00:00:00Z"]
        assert offset == {
            "max_cursor_value": "2026-01-01T00:00:00Z",
            "max_tiebreak_value": "x",
        }

    def test_snapshot_unaffected_by_missing_tiebreaker(self):
        """Snapshot mode (no cursor_field) never needs a tiebreaker, so a
        no-PK / multi-col-PK table still reads fine in snapshot."""
        c = self._connector(["a", "b"])
        recs = [{"ts": "2026-01-01T00:00:00Z"}]
        with patch.object(c, "_fetch_page", return_value=(recs, None)):
            emitted_iter, offset = c.read_table("T", None, {})
            emitted = list(emitted_iter)
        assert len(emitted) == 1
        assert offset == {}

    def test_incremental_sends_composite_orderby(self):
        """F-TEST-1: an incremental resume must request server-side sort by
        (cursor ASC, tiebreaker ASC) — the precondition that makes the
        composite where-clause resume tie-safe. Drives the real _fetch_page
        (mocking _session.post) so the posted orderBy body is asserted."""
        c = self._connector("row_id")  # single-col PK → tiebreak=row_id
        page = _mock_response(
            200,
            {"data": [{"arrivalTimestamp": "2026-01-02T00:00:00Z", "row_id": "z"}],
             "nextPageToken": None},
        )
        resume_offset = {
            "max_cursor_value": "2026-01-01T00:00:00Z",
            "max_tiebreak_value": "a",
        }
        with patch.object(
            c, "_get_max_cursor_value", return_value="2026-01-02T00:00:00Z"
        ), patch.object(
            c._session, "post", side_effect=[page]
        ) as post_spy, patch.object(time, "sleep"):
            emitted_iter, _ = c.read_table(
                "T", resume_offset, {"cursor_field": "arrivalTimestamp"}
            )
            list(emitted_iter)
        body = post_spy.call_args_list[0].kwargs["json"]
        assert body["orderBy"]["fields"] == [
            {"field": "arrivalTimestamp", "direction": "asc"},
            {"field": "row_id", "direction": "asc"},
        ]

    def test_incremental_refuses_null_tiebreaker_value(self):
        """F1: a resolved tiebreaker whose VALUE is null can't provide a
        total order — refuse rather than persist max_tiebreak_value=None
        (which would collapse the next run to lossy strict-gt)."""
        c = self._connector("row_id")
        recs = [{"arrivalTimestamp": "2026-01-01T00:00:00Z", "row_id": None}]
        with patch.object(c, "_fetch_page", return_value=(recs, None)):
            with pytest.raises(ValueError, match="[Tt]iebreaker"):
                c.read_table("T", None, {"cursor_field": "arrivalTimestamp"})
