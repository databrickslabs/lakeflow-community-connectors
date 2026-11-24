"""Mailchimp source connector."""

from databricks.labs.community_connector.sources.mailchimp.mailchimp import (
    MailchimpLakeflowConnect,
)

from databricks.labs.community_connector.sparkpds import LakeflowSource


class MailchimpDataSource(LakeflowSource):
    _lakeflow_connect_cls = MailchimpLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "mailchimp"


__all__ = [
    "MailchimpLakeflowConnect",
    "MailchimpDataSource",
]
