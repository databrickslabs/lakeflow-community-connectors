"""PayPal source connector."""

from databricks.labs.community_connector.sources.paypal.paypal import PaypalLakeflowConnect
from databricks.labs.community_connector.sparkpds import LakeflowSource


class PaypalDataSource(LakeflowSource):
    _lakeflow_connect_cls = PaypalLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "paypal"


__all__ = [
    "PaypalLakeflowConnect",
    "PaypalDataSource",
]
