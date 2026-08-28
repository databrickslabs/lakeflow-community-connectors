from databricks.labs.community_connector.sources.mailchimp.mailchimp import (
    MailchimpLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestMailchimpConnector(LakeflowConnectTests):
    connector_class = MailchimpLakeflowConnect
    # Simulate mode by default: spec + corpus live at
    # ``source_simulator/specs/mailchimp/``. The stand-in ``api_key`` only
    # needs a ``-<dc>`` suffix so ``__init__`` can derive the (unused, in
    # simulate mode) base URL; the simulator never validates the key.
    simulator_source = "mailchimp"
    replay_config = {"api_key": "0123456789abcdef0123456789abcde-us1"}
    sample_records = 5
    # ``members`` is nested per-list: the synthesized corpus may not seed
    # members under the first audience's ``list_id`` window, so an empty
    # first read is a fixture limitation, not a connector bug.
    allow_empty_first_read = frozenset({"members"})
