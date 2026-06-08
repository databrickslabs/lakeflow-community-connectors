from pathlib import Path

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


class DummyOverrideFormat(LakeflowSource):
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


def test_default_format_name_is_lakeflow_connect():
    """The base class supplies the format name UC injection requires."""
    assert DummyDataSource.name() == "lakeflow_connect"
    assert LakeflowSource._format_name == "lakeflow_connect"


def test_format_name_can_be_overridden():
    """A subclass may still override _format_name when it doesn't rely on UC."""
    assert DummyOverrideFormat.name() == "dummy"


def test_gmail_uses_default_format_name():
    """Gmail inherits the default ``lakeflow_connect`` format so its wheel
    registration matches the merged-file deployment and UC injection."""
    from databricks.labs.community_connector.sources.gmail import GmailDataSource

    assert GmailDataSource.name() == "lakeflow_connect"


def test_base_lakeflow_source_rejects_direct_instantiation():
    with pytest.raises(TypeError, match="_lakeflow_connect_cls"):
        LakeflowSource({})


def test_find_data_source_returns_class_for_gmail():
    # End-to-end: the gmail package exposes GmailDataSource as documented.
    ds_cls = registry.find_data_source("gmail")
    assert issubclass(ds_cls, LakeflowSource)
    assert ds_cls.name() == "lakeflow_connect"


# ---------------------------------------------------------------------------
# Per-source conformance: every NEW source must expose a `<Source>DataSource`
# that find_data_source() can locate. Sources that pre-date this contract are
# listed in _PENDING_MIGRATION and skip the check.  The set is intentionally
# frozen — a new source that hasn't migrated will fail the test, forcing the
# author to either add a DataSource subclass to their package's __init__.py
# (preferred) or explicitly opt out by editing this list.
# ---------------------------------------------------------------------------

_SOURCES_DIR = Path(__file__).resolve().parents[2] / "src" / "databricks" / "labs" / "community_connector" / "sources"

_PENDING_MIGRATION = frozenset({
    "actitime",
    "adme",
    "appsflyer",
    "azure_devops",
    "dicomweb",
    "fhir",
    "github",
    "google_analytics_aggregated",
    "google_sheets_docs",
    "hubspot",
    "microsoft_teams",
    "mixpanel",
    "osipi",
    "qualtrics",
    "sap_successfactors",
    "shopify",
    "surveymonkey",
    "zendesk",
    "zoho_crm",
})


def _all_source_names() -> list[str]:
    """All source-package names that have a primary connector module."""
    return sorted(
        d.name
        for d in _SOURCES_DIR.iterdir()
        if d.is_dir() and (d / f"{d.name}.py").exists()
    )


@pytest.mark.parametrize("source_name", _all_source_names())
def test_find_data_source_conformance(source_name):
    """Every non-grandfathered source must expose a `<Source>DataSource`.

    The Spark format name defaults to ``"lakeflow_connect"`` because UC
    connection injection looks for that exact string; we assert it here so
    a future refactor doesn't quietly drop UC compatibility.
    """
    if source_name in _PENDING_MIGRATION:
        pytest.skip(f"{source_name} pre-dates the <Source>DataSource contract")
    ds_cls = registry.find_data_source(source_name)
    assert issubclass(ds_cls, LakeflowSource)
    assert ds_cls._lakeflow_connect_cls is not None, (
        f"{ds_cls.__name__} must set _lakeflow_connect_cls"
    )
    assert ds_cls.name() == "lakeflow_connect", (
        f"{ds_cls.__name__}.name() must be 'lakeflow_connect' so Unity "
        f"Catalog connection-option injection works; got {ds_cls.name()!r}"
    )


def test_pending_migration_list_has_no_strays():
    """A source that has migrated should be removed from the exempt list."""
    sources_on_disk = set(_all_source_names())
    stale = _PENDING_MIGRATION - sources_on_disk
    assert not stale, (
        f"Pending-migration list references sources that no longer exist: {stale}. "
        f"Remove them from _PENDING_MIGRATION."
    )
