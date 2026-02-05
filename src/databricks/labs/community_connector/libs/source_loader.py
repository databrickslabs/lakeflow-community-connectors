"""Dynamically loads LakeFlow connector source modules."""
import importlib
import inspect
from types import ModuleType
from typing import Type

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect


_BASE_PKG = "databricks.labs.community_connector.sources"


def _get_source_module(source_name: str, module_name: str) -> ModuleType:
    """
    Helper function to import a module from a source package.

    Args:
        source_name: The name of the source (e.g., "zendesk", "github")
        module_name: The name of the module within the source package

    Returns:
        The imported module

    Raises:
        ValueError: If the source package does not exist
        ImportError: If the module cannot be imported
    """
    # Check if the source package exists
    try:
        importlib.import_module(f"{_BASE_PKG}.{source_name}")
    except ModuleNotFoundError:
        raise ValueError(
            f"Source '{source_name}' not found. "
            f"Make sure the directory 'src/databricks/labs/community_connector/sources/{source_name}/' exists."
        )

    # Import the specified module
    module_path = f"{_BASE_PKG}.{source_name}.{module_name}"
    try:
        return importlib.import_module(module_path)
    except ModuleNotFoundError:
        raise ImportError(
            f"Could not import '{module_name}.py' from source '{source_name}'. "
            f"Please ensure 'src/databricks/labs/community_connector/sources/{source_name}/{module_name}.py' exists."
        )


def get_lakeflow_connect_class(source_name: str) -> Type[LakeflowConnect]:
    """
    Dynamically imports and returns the LakeflowConnect implementation class
    from the specified source module.

    The function looks for a class named 'LakeflowConnect' in:
    - databricks.labs.community_connector.sources.{source_name}.{source_name}

    Args:
        source_name: The name of the source (e.g., "zendesk", "github")

    Returns:
        The LakeflowConnect implementation class from the specific source module

    Raises:
        ValueError: If the source module cannot be imported
        ImportError: If no LakeflowConnect implementation is found
        ImportError: If multiple LakeflowConnect implementations are found

    Example:
        >>> lakeflow_connect_cls = get_lakeflow_connect_class("github")
        >>> connector = lakeflow_connect_cls(options)
    """
    module = _get_source_module(source_name, source_name)
    module_path = f"{_BASE_PKG}.{source_name}.{source_name}"

    # Find all classes named 'LakeflowConnect' defined in this module
    lakeflow_connect_classes = []
    for name, obj in inspect.getmembers(module, inspect.isclass):
        # Only consider classes defined in this module (not imported)
        if obj.__module__ == module_path and name == "LakeflowConnect":
            lakeflow_connect_classes.append(obj)

    if len(lakeflow_connect_classes) == 0:
        raise ImportError(
            f"No LakeflowConnect implementation found in module '{source_name}.py'. "
            f"Please ensure the module defines a class named 'LakeflowConnect'."
        )

    if len(lakeflow_connect_classes) > 1:
        raise ImportError(
            f"Multiple LakeflowConnect implementations found in module '{source_name}.py': "
            f"{[cls.__name__ for cls in lakeflow_connect_classes]}. "
            f"Please ensure only one class named 'LakeflowConnect' is defined."
        )

    return lakeflow_connect_classes[0]


def get_register_function(source_name: str):
    """
    Dynamically imports and returns the register_lakeflow_source function
    from the specified source module.

    The function looks for the register_lakeflow_source function in:
    - databricks.labs.community_connector.sources.{source_name}._generated_{source_name}_python_source

    Args:
        source_name: The name of the source (e.g., "zendesk", "example")

    Returns:
        The register_lakeflow_source function from the specific source module

    Raises:
        ValueError: If the source module cannot be imported
        ImportError: If the register_lakeflow_source function is not found in the module

    Example:
        >>> register_fn = get_register_function("zendesk")
        >>> register_fn(spark)
    """
    module_name = f"_generated_{source_name}_python_source"
    module = _get_source_module(source_name, module_name)

    # Check if the module has the register function
    if not hasattr(module, "register_lakeflow_source"):
        raise ImportError(
            f"Module '{module_name}' does not have a 'register_lakeflow_source' function. "
            f"Please ensure the module defines this function."
        )

    return module.register_lakeflow_source
