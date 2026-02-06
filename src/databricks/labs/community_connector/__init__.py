"""Lakeflow Community Connectors - Built on Spark Python Data Source API."""


def __getattr__(name):
    """Lazy import to avoid importing pyspark-dependent modules at package init time."""
    if name == "register":
        from databricks.labs.community_connector.sparkpds.registry import register

        return register
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["register"]