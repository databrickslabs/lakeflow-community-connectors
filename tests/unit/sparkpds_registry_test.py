import pytest
from unittest.mock import patch, MagicMock

import databricks.labs.community_connector.sparkpds.registry as registry
from databricks.labs.community_connector.interface import LakeflowConnect
from pyspark.sql.datasource import DataSource


class DummyLakeflowConnect(LakeflowConnect):
    pass


class DummyDataSource(DataSource):
    pass


def _registered_class_names(mock_spark):
    return [
        call.args[0].__name__
        for call in mock_spark.dataSource.register.call_args_list
    ]


def test_datasource_subclass_register():
    mock_spark = MagicMock()
    registry.register(mock_spark, DummyDataSource)
    mock_spark.dataSource.register.assert_called_once_with(DummyDataSource)


def test_lakeflow_connect_subclass_register():
    mock_spark = MagicMock()
    with patch.object(registry, "_import_class", return_value=DummyLakeflowConnect):
        registry.register(mock_spark, DummyLakeflowConnect)

    # The single registered LakeflowSource serves both the table-mode and
    # the agent-operation-mode dispatch paths under the lakeflow_connect format.
    assert _registered_class_names(mock_spark) == [
        "RegisterableLakeflowSource_DummyLakeflowConnect",
    ]


@patch.object(registry, "_get_register_function", side_effect=ImportError("no generated module"))
@patch.object(registry, "_find_lakeflow_connect_class", return_value=DummyLakeflowConnect)
@patch.object(registry, "_import_class", return_value=DummyLakeflowConnect)
def test_string_source_fallback_to_lakeflow_connect(mock_import_cls, mock_find_cls, mock_get_reg):
    mock_spark = MagicMock()

    registry.register(mock_spark, "dummy_source")

    mock_get_reg.assert_called_once_with("dummy_source")
    mock_find_cls.assert_called_once_with("dummy_source")

    assert _registered_class_names(mock_spark) == [
        "RegisterableLakeflowSource_DummyLakeflowConnect",
    ]


@patch.object(registry, "_find_lakeflow_connect_class", return_value=DummyLakeflowConnect)
@patch.object(registry, "_import_class", return_value=DummyLakeflowConnect)
@patch.object(registry, "_get_register_function")
def test_string_source_invokes_generated_module_then_overlays_class_discovery(
    mock_get_reg, mock_import_cls, mock_find_cls
):
    """Generated module is called for SDP-merged sources; class-discovery
    LakeflowSource is then registered on top so the agent-operation path
    is available even for sources whose generated module ships the
    pre-fold LakeflowSource shape.
    """
    mock_spark = MagicMock()
    mock_register_fn = MagicMock()
    mock_get_reg.return_value = mock_register_fn

    registry.register(mock_spark, "zendesk")

    mock_get_reg.assert_called_once_with("zendesk")
    mock_register_fn.assert_called_once_with(mock_spark)
    # Generated module's register_fn is a mock that no-ops on the spark
    # double — only the class-discovery wrapper registers via dataSource.
    assert _registered_class_names(mock_spark) == [
        "RegisterableLakeflowSource_DummyLakeflowConnect",
    ]


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
