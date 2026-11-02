from databricks.labs.community_connector.sources.economic.economic import (
    EconomicLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestEconomicConnector(LakeflowConnectTests):
    connector_class = EconomicLakeflowConnect
    # Simulate mode by default: spec + corpus live at
    # ``source_simulator/specs/economic/``. The custom ``cursor_list``
    # handler evaluates e-conomic ``filter=`` expressions and serves
    # body-cursor pages. The cdc specs declare ``synthesize_future_records``
    # so ``test_read_terminates`` checks the connector's init-time cap.
    simulator_source = "economic"
    # Stand-in credentials; the simulator never validates them. ("demo" /
    # "demo" is also e-conomic's public read-only demo agreement.)
    replay_config = {
        "app_secret_token": "demo",
        "agreement_grant_token": "demo",
    }
    # 8, not 5: the corpus is the live demo rows (5 customers, 7 projects)
    # followed by synthetic rows; record 6/8 is the first synthetic row,
    # the only one that populates the columns the demo agreement leaves null.
    sample_records = 8
    # The public demo agreement is tiny (5-10 rows per table, one page
    # each), so keep whole live responses in the cassette; the corpus is
    # re-seeded from it via ``tools.cassette_to_corpus``.
    record_replay_sample_size = 1000
    # Small batches on the cdc tables so the read tests exercise the
    # connector's resumable scan (``phase=scanning`` + ``number$gt:<pk>``
    # filter clause). Live, the 10-row demo time-entries table is cut at 7,
    # so the resume call runs against the real API. projects is 8 so its
    # first batch reaches the first synthetic row after the 7 live demo
    # rows (no demo project has a departmentNumber).
    table_configs = {
        "projects": {"max_records_per_batch": "8"},
        "project_time_entries": {"max_records_per_batch": "7"},
    }
