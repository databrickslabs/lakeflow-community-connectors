import pytest
from unittest.mock import patch, MagicMock

import databricks.labs.community_connector.sparkpds.registry as registry
from databricks.labs.community_connector.interface import LakeflowConnect
from pyspark.sql.datasource import DataSource


class DummyLakeflowConnect(LakeflowConnect):
    pass


class DummyDataSource(DataSource):
    pass


def test_datasource_subclass_register():
    mock_spark = MagicMock()
    registry.register(mock_spark, DummyDataSource)
    mock_spark.dataSource.register.assert_called_once_with(DummyDataSource)


def test_lakeflow_connect_subclass_register():
    mock_spark = MagicMock()
    with patch.object(registry, "_import_class", return_value=DummyLakeflowConnect):
        registry.register(mock_spark, DummyLakeflowConnect)

    mock_spark.dataSource.register.assert_called_once()
    registered_cls = mock_spark.dataSource.register.call_args[0][0]
    assert registered_cls.__name__ == "RegisterableLakeflowSource_DummyLakeflowConnect"


@patch.object(registry, "_get_register_function", side_effect=ImportError("no generated module"))
@patch.object(registry, "_find_lakeflow_connect_class", return_value=DummyLakeflowConnect)
@patch.object(registry, "_import_class", return_value=DummyLakeflowConnect)
def test_string_source_fallback_to_lakeflow_connect(mock_import_cls, mock_find_cls, mock_get_reg):
    mock_spark = MagicMock()

    registry.register(mock_spark, "dummy_source")

    mock_get_reg.assert_called_once_with("dummy_source")
    mock_find_cls.assert_called_once_with("dummy_source")

    mock_spark.dataSource.register.assert_called_once()
    registered_cls = mock_spark.dataSource.register.call_args[0][0]
    assert registered_cls.__name__ == "RegisterableLakeflowSource_DummyLakeflowConnect"


@patch.object(registry, "_get_register_function")
def test_string_source_uses_generated_module(mock_get_reg):
    mock_spark = MagicMock()
    mock_register_fn = MagicMock()
    mock_get_reg.return_value = mock_register_fn

    registry.register(mock_spark, "zendesk")

    mock_get_reg.assert_called_once_with("zendesk")
    mock_register_fn.assert_called_once_with(mock_spark)


def test_invalid_source_raises_type_error():
    mock_spark = MagicMock()
    with pytest.raises(TypeError, match="source must be a string"):
        registry.register(mock_spark, 42)


@patch.object(registry, "_get_register_function", side_effect=ImportError("no generated module"))
@patch.object(registry, "_find_lakeflow_connect_class", side_effect=ValueError("not found"))
def test_string_source_fallback_raises_when_no_class_found(mock_find_cls, mock_get_reg):
    mock_spark = MagicMock()
    with pytest.raises(ValueError, match="not found"):
        registry.register(mock_spark, "nonexistent")
