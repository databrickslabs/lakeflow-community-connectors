from pathlib import Path

from databricks.labs.community_connector.libs.simulated_source.api import reset_api
from databricks.labs.community_connector.sources.example.example import ExampleLakeflowConnect
from tests.unit.sources.test_suite import create_test_class
from tests.unit.sources.test_utils import load_config

config_dir = Path(__file__).parent / "configs"
config = load_config(config_dir / "dev_config.json")
table_config = load_config(config_dir / "dev_table_config.json")


TestExampleConnector = create_test_class(
    connector_class=ExampleLakeflowConnect,
    config=config,
    table_configs=table_config,
    sample_records=100,
    setup_fn=lambda: reset_api(config["username"], config["password"]),
)
