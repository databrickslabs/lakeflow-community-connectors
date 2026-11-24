import pytest
from unittest.mock import patch, MagicMock

import databricks.labs.community_connector.sparkpds.registry as registry
from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowSource
from pyspark.sql.datasource import DataSource


class DummyLakeflowConnect(LakeflowConnect):
    pass


class DummyDataSource(DataSource):
    pass


def test_datasource_class_is_not_accepted_by_register():
    # register() is a name-only legacy shim. A DataSource class must be
    # registered via spark.dataSource.register(...) directly, not through here.
    mock_spark = MagicMock()
    with pytest.raises(TypeError, match="register\\(\\) takes a source name string"):
        registry.register(mock_spark, DummyDataSource)
    mock_spark.dataSource.register.assert_not_called()


def test_string_source_uses_generated_module():
    mock_spark = MagicMock()
    mock_register_fn = MagicMock()
    with patch.object(registry, "_get_register_function", return_value=mock_register_fn) as mock_get_reg:
        registry.register(mock_spark, "zendesk")

    mock_get_reg.assert_called_once_with("zendesk")
    mock_register_fn.assert_called_once_with(mock_spark)


@patch.object(registry, "find_data_source")
@patch.object(registry, "_get_register_function", side_effect=ModuleNotFoundError("no generated module"))
def test_string_source_does_not_fall_back_to_find_data_source(mock_get_reg, mock_find):
    # The string path is the legacy merged-module path only; it must NOT fall
    # back to find_data_source. The lookup error propagates unchanged.
    mock_spark = MagicMock()
    with pytest.raises(ModuleNotFoundError, match="no generated module"):
        registry.register(mock_spark, "dummy_source")

    mock_find.assert_not_called()
    mock_spark.dataSource.register.assert_not_called()


@patch.object(registry, "find_data_source")
@patch.object(registry, "_get_register_function", side_effect=AttributeError("no register fn"))
def test_string_source_propagates_missing_register_fn(mock_get_reg, mock_find):
    mock_spark = MagicMock()
    with pytest.raises(AttributeError, match="no register fn"):
        registry.register(mock_spark, "dummy_source")

    mock_find.assert_not_called()
    mock_spark.dataSource.register.assert_not_called()


def test_lakeflow_connect_class_is_not_accepted_by_register():
    # register() takes a name string only; classes (of any kind) are rejected.
    mock_spark = MagicMock()
    with pytest.raises(TypeError, match="register\\(\\) takes a source name string"):
        registry.register(mock_spark, DummyLakeflowConnect)


def test_non_string_source_raises_type_error():
    mock_spark = MagicMock()
    with pytest.raises(TypeError, match="register\\(\\) takes a source name string"):
        registry.register(mock_spark, 42)


def test_find_data_source_returns_source_subclass():
    ds = registry.find_data_source("example")
    assert issubclass(ds, LakeflowSource)
    assert ds is not LakeflowSource
    assert ds._lakeflow_connect_cls is not None
    # Format name stays "lakeflow_connect" until a source opts out of UC injection.
    assert ds.name() == "lakeflow_connect"


def test_find_data_source_missing_source_raises():
    with pytest.raises(ValueError, match="not found"):
        registry.find_data_source("nonexistent_source_xyz")


def test_find_data_source_respects_package_boundary():
    # A class from a sibling package whose name is a prefix-superset
    # ("github_enterprise" vs "github") must not satisfy the lookup for the
    # shorter name, even when it sorts first in getmembers() iteration.
    import types

    class AaaEnterpriseDataSource(LakeflowSource):  # sorts first
        _lakeflow_connect_cls = object
    AaaEnterpriseDataSource.__module__ = f"{registry._BASE_PKG}.github_enterprise.x"

    class ZzzGithubDataSource(LakeflowSource):
        _lakeflow_connect_cls = object
    ZzzGithubDataSource.__module__ = f"{registry._BASE_PKG}.github"

    fake_pkg = types.ModuleType("fake_github_pkg")
    fake_pkg.AaaEnterpriseDataSource = AaaEnterpriseDataSource
    fake_pkg.ZzzGithubDataSource = ZzzGithubDataSource

    with patch.object(registry.importlib, "import_module", return_value=fake_pkg):
        assert registry.find_data_source("github") is ZzzGithubDataSource
