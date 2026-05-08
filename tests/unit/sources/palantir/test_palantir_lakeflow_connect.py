from unittest.mock import patch

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

    def test_where_clause_built_for_incremental_call(self):
        """When ``start_offset`` carries a ``max_cursor_value`` older
        than the value returned by the ``aggregate`` endpoint, the
        connector's incremental path must build a server-side
        ``where: gt`` filter on ``cursor_field`` and pass it to
        ``loadObjects`` via ``_build_object_set``.

        ``test_read_terminates`` does not exercise this branch in
        simulate mode because the static ``aggregate`` corpus returns
        the same value every call, so ``new_max_cursor == prev`` and
        ``_read_incremental`` early-returns before reaching the
        where-clause builder. This test forces the branch by handing
        in a deliberately stale offset.
        """
        table = self.connector.list_tables()[0]
        cursor_field = "arrivalTimestamp"
        options = {"cursor_field": cursor_field, "page_size": "100"}
        stale_offset = {"max_cursor_value": "1970-01-01T00:00:00Z"}

        original = self.connector._fetch_page
        with patch.object(
            self.connector, "_fetch_page", wraps=original
        ) as fetch_page_spy:
            records, _ = self.connector.read_table(
                table, stale_offset, options
            )
            list(records)  # drain generator so _fetch_page is called

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

        ``search`` is a fallback inside ``_get_max_cursor_via_search``
        that only fires when ``aggregate`` returns ``None`` (e.g. for
        aggregation-disabled or empty Palantir object types). For
        tables that support aggregation (the common case), the
        fallback is never reached via the normal read path, leaving
        the endpoint un-hit in coverage reports. This test issues a
        direct call so coverage is complete in both simulate and
        live runs.
        """
        tables = self.connector.list_tables()
        assert tables, "Palantir ontology returned no tables"
        table = tables[0]
        cursor_field = self.connector.get_table_schema(
            table, {}
        ).fieldNames()[0]
        # Returns None or a value; either is acceptable — we only care
        # that POST /objects/{table}/search was issued.
        self.connector._get_max_cursor_via_search(table, cursor_field)


class TestPalantirMaxCursorFallback:
    """Unit-level coverage for the aggregate→search fallback path that
    live tests can't easily reach (FlightsFinal supports aggregation, so
    the fallback never fires in record mode).
    """

    @staticmethod
    def _connector() -> PalantirLakeflowConnect:
        return PalantirLakeflowConnect({
            "token": "fake",
            "hostname": "fake.palantirfoundry.com",
            "ontology_api_name": "ontology-fake",
        })

    def test_search_fallback_when_aggregate_returns_none(self):
        c = self._connector()
        with patch.object(c, "_get_max_cursor_via_aggregate", return_value=None) as agg, \
             patch.object(c, "_get_max_cursor_via_search", return_value="2026-01-01") as srch:
            result = c._get_max_cursor_value("FlightsFinal", "date")
        assert result == "2026-01-01"
        agg.assert_called_once_with("FlightsFinal", "date")
        srch.assert_called_once_with("FlightsFinal", "date")

    def test_search_not_called_when_aggregate_returns_value(self):
        c = self._connector()
        with patch.object(c, "_get_max_cursor_via_aggregate", return_value="2026-02-02") as agg, \
             patch.object(c, "_get_max_cursor_via_search") as srch:
            result = c._get_max_cursor_value("FlightsFinal", "date")
        assert result == "2026-02-02"
        agg.assert_called_once_with("FlightsFinal", "date")
        srch.assert_not_called()


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

    def test_iso_timestamp_detection(self):
        c = self._connector()
        assert c._is_iso_timestamp("2026-05-09T13:30:00Z") is True
        assert c._is_iso_timestamp("2026-05-09T13:30:00.123456+00:00") is True
        assert c._is_iso_timestamp("2026-05-09") is True
        # Non-timestamp values
        assert c._is_iso_timestamp(12345) is False
        assert c._is_iso_timestamp(123.45) is False
        assert c._is_iso_timestamp("abc-de-fg") is False
        assert c._is_iso_timestamp("flight_id_123") is False
        assert c._is_iso_timestamp("") is False
        assert c._is_iso_timestamp(None) is False
        # Plausible UUID-ish: not ISO-shaped
        assert c._is_iso_timestamp("550e8400-e29b-41d4-a716-446655440000") is False

    def test_numeric_cursor_does_not_crash_on_cap(self):
        """When cursor is an int (e.g. auto-incrementing ID), the cap
        comparison ``int > str(_init_time)`` would raise TypeError if
        not gated. The connector must skip the cap and proceed."""
        c = self._connector()
        with patch.object(
            c, "_get_max_cursor_via_aggregate", return_value=99999
        ), patch.object(
            c, "_fetch_page", return_value=([], None)
        ):
            # First call: prev=None, current=99999. _is_iso_timestamp(99999)
            # is False, so the cap branch is skipped — no TypeError.
            records, offset = c.read_table(
                "FlightsFinal", None, {"cursor_field": "row_id"}
            )
            list(records)
        assert offset == {"max_cursor_value": 99999}

    def test_type_mismatched_max_uses_current(self):
        """When prev/current cursor types differ (typically because the
        cursor field's type changed upstream), max() would raise. The
        connector falls back to the current value rather than crashing."""
        c = self._connector()
        with patch.object(
            c, "_get_max_cursor_via_aggregate", return_value=1000
        ), patch.object(
            c, "_fetch_page", return_value=([], None)
        ):
            stale_string_offset = {"max_cursor_value": "2024-01-01T00:00:00Z"}
            records, offset = c.read_table(
                "FlightsFinal", stale_string_offset, {"cursor_field": "row_id"}
            )
            list(records)
        # Current (int 1000) wins over stale string offset.
        assert offset == {"max_cursor_value": 1000}
