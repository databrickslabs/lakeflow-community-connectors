from databricks.labs.community_connector.sources.surveymonkey.surveymonkey import SurveymonkeyLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestSurveymonkeyConnector(LakeflowConnectTests):
    connector_class = SurveymonkeyLakeflowConnect
