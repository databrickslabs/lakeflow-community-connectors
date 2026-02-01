from pathlib import Path


def get_register_function(source_name: str):
    """
    Dynamically loads and returns the register_lakeflow_source function
    from the specified source module.

    Uses exec() to load the code into the caller's namespace, avoiding
    module import issues on Spark workers. This makes the classes available
    in the main execution context where they can be pickled and unpickled
    without module path dependencies.

    The function looks for the register_lakeflow_source function in:
    - sources/{source_name}/_generated_{source_name}_python_source.py

    Args:
        source_name: The name of the source (e.g., "zendesk", "example")

    Returns:
        The register_lakeflow_source function from the specific source module

    Raises:
        ValueError: If the source file cannot be found
        ImportError: If the register_lakeflow_source function is not found

    Example:
        >>> register_fn = get_register_function("zendesk")
        >>> register_fn(spark)
        
    Note:
        This function uses exec() to load the code, which means all classes
        and functions from the generated file will be available in the
        caller's global namespace. This is intentional to avoid module
        import issues when Spark distributes code to workers.
    """
    # Construct the file path
    source_dir = Path(__file__).parent.parent / "sources" / source_name
    generated_file = source_dir / f"_generated_{source_name}_python_source.py"

    # Check if file exists
    if not generated_file.exists():
        raise ValueError(
            f"Source '{source_name}' generated file not found. "
            f"Expected: {generated_file}\n"
            f"Please ensure the file exists."
        )

    # Read the file content
    with open(generated_file, 'r') as f:
        code = f.read()
    
    # Execute in the caller's global namespace
    # This is done by getting the caller's globals from the call stack
    import inspect
    caller_globals = inspect.currentframe().f_back.f_globals
    
    # Execute the code in caller's namespace
    # This makes all classes/functions available without module imports
    exec(code, caller_globals)
    
    # The register_lakeflow_source function should now be in caller's globals
    if 'register_lakeflow_source' not in caller_globals:
        raise ImportError(
            f"Module '{generated_file.name}' does not define 'register_lakeflow_source'. "
            f"Please ensure the module defines this function."
        )
    
    return caller_globals['register_lakeflow_source']
