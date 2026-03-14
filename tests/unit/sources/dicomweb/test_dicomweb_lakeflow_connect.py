from databricks.labs.community_connector.sources.dicomweb.dicomweb import DICOMwebLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestDICOMwebConnector(LakeflowConnectTests):
    connector_class = DICOMwebLakeflowConnect
