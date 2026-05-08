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
