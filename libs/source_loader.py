import importlib.util
import sys
from pathlib import Path


def get_register_function(source_name: str):
    """
    Dynamically loads and returns the register_lakeflow_source function
    from the specified source module.

    Uses importlib.util.spec_from_file_location to load the module directly
    without requiring package structure. This avoids ModuleNotFoundError on
    Spark workers that don't have access to the 'sources' package.

    The function looks for the register_lakeflow_source function in:
    - sources/{source_name}/_generated_{source_name}_python_source.py

    Args:
        source_name: The name of the source (e.g., "lancedb", "zendesk")

    Returns:
        The register_lakeflow_source function from the specific source module

    Raises:
        ValueError: If the source file cannot be found
        ImportError: If the register_lakeflow_source function is not found in the module

    Example:
        >>> register_fn = get_register_function("lancedb")
        >>> register_fn(spark)
    """
    # Construct the file path
    # Assume we're in the project root or adjust path as needed
    source_dir = Path(__file__).parent.parent / "sources" / source_name
    generated_file = source_dir / f"_generated_{source_name}_python_source.py"

    # Check if file exists
    if not generated_file.exists():
        raise ValueError(
            f"Source '{source_name}' generated file not found. "
            f"Expected: {generated_file}\n"
            f"Please ensure the file exists."
        )

    # Load module directly from file (no package import!)
    module_name = f"_generated_{source_name}_python_source"
    spec = importlib.util.spec_from_file_location(module_name, generated_file)
    
    if spec is None or spec.loader is None:
        raise ImportError(
            f"Could not load module spec from {generated_file}"
        )
    
    module = importlib.util.module_from_spec(spec)
    
    # CRITICAL: Add to sys.modules to make it findable during unpickling
    # Use a simple name without package path to avoid 'sources' dependency
    sys.modules[module_name] = module
    
    # Execute the module
    spec.loader.exec_module(module)

    # Check if the module has the register function
    if not hasattr(module, "register_lakeflow_source"):
        raise ImportError(
            f"Module '{generated_file.name}' does not have a 'register_lakeflow_source' function. "
            f"Please ensure the module defines this function."
        )

    return module.register_lakeflow_source
