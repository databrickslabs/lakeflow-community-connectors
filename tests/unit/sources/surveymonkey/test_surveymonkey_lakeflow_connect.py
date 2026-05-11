from databricks.labs.community_connector.sources.surveymonkey.surveymonkey import SurveymonkeyLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestSurveymonkeyConnector(LakeflowConnectTests):
    connector_class = SurveymonkeyLakeflowConnect
    simulator_source = "surveymonkey"
    replay_config = {
        "access_token": "simulator-fake-access-token",
        "base_url": "https://simulator.example.com/v3",
    }
