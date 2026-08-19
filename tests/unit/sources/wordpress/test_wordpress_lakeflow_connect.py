from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests

from databricks.labs.community_connector.sources.wordpress.wordpress import (
    WordPressLakeflowConnect,
)


class TestWordPressConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    """Simulate-mode suite for the WordPress ``wp/v2`` connector.

    Runs offline against ``source_simulator/specs/wordpress/`` (endpoints.yaml
    + corpus). ``posts`` / ``pages`` / ``media`` / ``comments`` are partitioned
    streaming tables; ``categories`` / ``tags`` / ``users`` / ``taxonomies`` are
    snapshot tables read via ``read_table``.
    """

    connector_class = WordPressLakeflowConnect
    simulator_source = "wordpress"

    # The simulator never validates credentials; any well-shaped strings work.
    # WordPress uses Application Password (HTTP Basic) auth.
    replay_config = {
        "base_url": "https://simulator.example.com",
        "username": "simulator-user",
        "application_password": "simu lato rfak epas",
    }
