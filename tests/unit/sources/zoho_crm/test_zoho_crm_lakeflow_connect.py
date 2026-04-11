from databricks.labs.community_connector.sources.zoho_crm.zoho_crm import ZohoCRMLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestZohoCrmConnector(LakeflowConnectTests):
    connector_class = ZohoCRMLakeflowConnect
