from databricks.labs.community_connector.sources.osipi.osipi import OsipiLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestOsipiConnector(LakeflowConnectTests):
    connector_class = OsipiLakeflowConnect
    simulator_source = "osipi"
    replay_config = {
        "pi_base_url": "https://simulator-pi.example.com",
        "access_token": "simulator-fake-access-token",
    }
    # Simulator corpus times don't overlap the connector's first-call
    # window for time-series tables — fixture limitation, not a bug.
    allow_empty_first_read = frozenset({
        "pi_timeseries", "pi_streamset_recorded", "pi_interpolated",
        "pi_streamset_interpolated", "pi_plot", "pi_streamset_plot",
        "pi_streamset_summary", "pi_calculated", "pi_event_frames",
        "pi_eventframe_referenced_elements",
    })
