import pytest
from unittest.mock import patch, MagicMock

import databricks.labs.community_connector.sparkpds.registry as registry
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowSource
from databricks.labs.community_connector.interface import LakeflowConnect
from pyspark.sql.datasource import DataSource


class DummyLakeflowConnect(LakeflowConnect):
    pass


class DummyDataSource(LakeflowSource):
    _lakeflow_connect_cls = DummyLakeflowConnect
    _format_name = "dummy"


class PlainDataSource(DataSource):
    pass


def test_datasource_subclass_register():
    mock_spark = MagicMock()
    registry.register(mock_spark, PlainDataSource)
    mock_spark.dataSource.register.assert_called_once_with(PlainDataSource)


def test_lakeflow_source_subclass_register():
    mock_spark = MagicMock()
    registry.register(mock_spark, DummyDataSource)
    mock_spark.dataSource.register.assert_called_once_with(DummyDataSource)


@patch.object(registry, "_get_register_function")
def test_string_source_uses_generated_module_first(mock_get_reg):
    mock_spark = MagicMock()
    mock_register_fn = MagicMock()
    mock_get_reg.return_value = mock_register_fn

    registry.register(mock_spark, "zendesk")

    mock_get_reg.assert_called_once_with("zendesk")
    mock_register_fn.assert_called_once_with(mock_spark)
    mock_spark.dataSource.register.assert_not_called()


@patch.object(registry, "_get_register_function", side_effect=ModuleNotFoundError)
@patch.object(registry, "find_data_source", return_value=DummyDataSource)
def test_string_source_falls_back_to_data_source_class(mock_find, mock_get_reg):
    mock_spark = MagicMock()
    registry.register(mock_spark, "dummy")

    mock_get_reg.assert_called_once_with("dummy")
    mock_find.assert_called_once_with("dummy")
    mock_spark.dataSource.register.assert_called_once_with(DummyDataSource)


@patch.object(registry, "_get_register_function", side_effect=AttributeError)
@patch.object(registry, "find_data_source", return_value=DummyDataSource)
def test_string_source_falls_back_when_register_fn_missing(mock_find, mock_get_reg):
    mock_spark = MagicMock()
    registry.register(mock_spark, "dummy")

    mock_find.assert_called_once_with("dummy")
    mock_spark.dataSource.register.assert_called_once_with(DummyDataSource)


def test_string_source_raises_when_package_missing():
    mock_spark = MagicMock()
    with pytest.raises(ValueError, match="Source 'nonexistent' not found"):
        registry.register(mock_spark, "nonexistent")


def test_invalid_source_raises_type_error():
    mock_spark = MagicMock()
    with pytest.raises(TypeError, match="source must be a string"):
        registry.register(mock_spark, 42)


def test_format_name_comes_from_class_attribute():
    assert DummyDataSource.name() == "dummy"


def test_base_lakeflow_source_rejects_direct_instantiation():
    with pytest.raises(TypeError, match="_lakeflow_connect_cls"):
        LakeflowSource({})


def test_find_data_source_returns_class_for_gmail():
    # End-to-end: the gmail package exposes GmailDataSource as documented.
    ds_cls = registry.find_data_source("gmail")
    assert issubclass(ds_cls, LakeflowSource)
    assert ds_cls.name() == "gmail"
