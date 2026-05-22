import time
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
        ``test_where_clause_built_for_incremental_call``) still call
        ``connector.list_tables()`` directly when they need to scan
        the full ontology.
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
        assert offset == {"max_cursor_value": "2026-05-09T08:00:00-05:00"}

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
        assert offset == {"max_cursor_value": 102}

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
        assert offset == {"max_cursor_value": "2026-05-08"}

    def test_early_exit_when_search_max_equals_prev_cursor(self):
        """Early-exit short-circuit fires when the search peek returns
        a value ``<= prev_max_cursor``. ``loadObjects`` must not be
        called and the offset must round-trip unchanged so Spark
        Streaming terminates the microbatch.
        """
        c = self._connector()
        prev = "2026-04-02T00:00:00Z"
        with patch.object(
            c, "_get_max_cursor_value", return_value=prev
        ) as search_spy, patch.object(
            c, "_fetch_page"
        ) as fetch_spy:
            emitted_iter, offset = c.read_table(
                "FlightsFinal",
                {"max_cursor_value": prev},
                {"cursor_field": "arrivalTimestamp"},
            )
            emitted = list(emitted_iter)
        assert emitted == []
        assert offset == {"max_cursor_value": prev}
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
        assert offset == {"max_cursor_value": "2026-04-03T00:00:00Z"}
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
        too: when the search peek returns an int ``<= prev``, the
        short-circuit fires. ``_to_utc_datetime`` returns ``None`` for
        non-strings, so the helper falls back to direct ``>``.
        """
        c = self._connector()
        with patch.object(
            c, "_get_max_cursor_value", return_value=100
        ), patch.object(c, "_fetch_page") as fetch_spy:
            emitted_iter, offset = c.read_table(
                "FlightsFinal",
                {"max_cursor_value": 100},
                {"cursor_field": "seq"},
            )
            assert list(emitted_iter) == []
        assert offset == {"max_cursor_value": 100}
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
        assert offset == {"max_cursor_value": "2026-04-02T00:00:00Z"}


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


def _mock_response(status: int, json_payload: dict = None):
    """Build a ``requests.Response``-like mock for ``_fetch_page``."""
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_payload or {}
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
            with pytest.raises(requests.HTTPError):
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
            with pytest.raises(requests.HTTPError):
                c._fetch_page({"type": "base", "objectType": "T"})
        assert post_spy.call_count == 1
        sleep_spy.assert_not_called()

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
