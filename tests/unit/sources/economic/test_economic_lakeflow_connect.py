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
    # Large enough that the column-population check reaches the synthetic
    # rows appended after the (sparse) live demo rows in the corpus.
    sample_records = 50
    # The public demo agreement is tiny (5-10 rows per table, one page
    # each), so keep whole live responses in the cassette; the corpus is
    # re-seeded from it via ``tools.cassette_to_corpus``.
    record_replay_sample_size = 1000
    # Small batches on the cdc tables so the read tests exercise the
    # connector's resumable scan (``phase=scanning`` + ``number$gt:<pk>``
    # filter clause) and cursor pagination together; the corpus has the
    # live demo rows (7 projects / 10 time entries) plus 15 synthetic rows
    # each (+3 future-dated clones) and the handler pages at 20 items. Live,
    # the 7-row demo projects table ends a batch exactly at the cap, so the
    # resume call (``number$gt:7``) is exercised against the real API too.
    table_configs = {
        "projects": {"max_records_per_batch": "7"},
        "project_time_entries": {"max_records_per_batch": "7"},
    }
