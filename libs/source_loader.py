import importlib


def get_register_function(source_name: str):
    """
    Dynamically imports and returns the register_lakeflow_source function
    from the specified source module.

    The function looks for the register_lakeflow_source function in:
    - sources.{source_name}._generated_{source_name}_python_source

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
    # Check if the source package exists
    try:
        importlib.import_module(f"sources.{source_name}")
    except ModuleNotFoundError:
        raise ValueError(
            f"Source '{source_name}' not found. "
            f"Make sure the directory 'sources/{source_name}/' exists."
        )

    # Import the generated module
    module_path = f"sources.{source_name}._generated_{source_name}_python_source"
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError:
        raise ImportError(
            f"Could not import '_generated_{source_name}_python_source.py' from source '{source_name}'. "
            f"Please ensure 'sources/{source_name}/_generated_{source_name}_python_source.py' exists."
        )

    # Check if the module has the register function
    if not hasattr(module, "register_lakeflow_source"):
        raise ImportError(
            f"Module '_generated_{source_name}_python_source' does not have a 'register_lakeflow_source' function. "
            f"Please ensure the module defines this function."
        )

    return module.register_lakeflow_source
