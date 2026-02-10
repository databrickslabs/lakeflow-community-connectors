#!/usr/bin/env python3
"""
Script to merge source files into a single deployable file.
Due to current limitations, the Spark Declarative Pipeline (SDP) does not
support module imports for Python Data Source implementations.

This script combines:
1. src/databricks/labs/community_connector/libs/utils.py (parsing utilities)
2. src/databricks/labs/community_connector/interface/lakeflow_connect.py (LakeflowConnect base class)
3. src/databricks/labs/community_connector/sources/{source_name}/*.py (source library files, in dependency order)
4. src/databricks/labs/community_connector/sources/{source_name}/{source_name}.py (main source connector implementation)
5. src/databricks/labs/community_connector/sparkpds/lakeflow_datasource.py (PySpark data source registration)

Source library files (e.g., github_utils.py, github_schemas.py) are automatically discovered
and included before the main source file. The script analyzes import dependencies to determine
the correct ordering. Cross-file imports within the source directory are automatically removed.

File Exclusion:
    Files can be excluded from merging via merge_exclude_config.json in this directory.
    The config supports:
    - global_exclude: Glob patterns applied to all sources (e.g., "*_test*.py", "*_oauth_setup.py")
    - source_exclude: Per-source exclusion patterns (e.g., {"zoho_crm": ["zoho_crm_oauth_setup.py"]})

Usage:
    python tools/scripts/merge_python_source.py <source_name>
    python tools/scripts/merge_python_source.py zendesk
    python tools/scripts/merge_python_source.py zendesk -o output/zendesk_merged.py
    python tools/scripts/merge_python_source.py all  # Regenerate all sources
"""

import argparse
import fnmatch
import json
import re
import sys
from pathlib import Path
from typing import List, Optional

# Get the project root directory
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
EXCLUDE_CONFIG_PATH = SCRIPT_DIR / "merge_exclude_config.json"


def load_exclude_config() -> dict:
    """
    Load the exclusion configuration from merge_exclude_config.json.

    Returns:
        Dictionary with 'global_exclude' and 'source_exclude' keys.
        Returns empty config if file doesn't exist.
    """
    if not EXCLUDE_CONFIG_PATH.exists():
        return {"global_exclude": [], "source_exclude": {}}

    try:
        with open(EXCLUDE_CONFIG_PATH, "r") as f:
            config = json.load(f)
        return {
            "global_exclude": config.get("global_exclude", []),
            "source_exclude": config.get("source_exclude", {}),
        }
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Failed to load exclude config: {e}", file=sys.stderr)
        return {"global_exclude": [], "source_exclude": {}}


def should_exclude_file(filename: str, source_name: str, exclude_config: dict) -> bool:
    """
    Check if a file should be excluded based on the exclusion configuration.

    Args:
        filename: Name of the file to check.
        source_name: Name of the source connector.
        exclude_config: Exclusion configuration dictionary.

    Returns:
        True if the file should be excluded, False otherwise.
    """
    # Check global exclusions
    for pattern in exclude_config.get("global_exclude", []):
        if fnmatch.fnmatch(filename, pattern):
            return True

    # Check source-specific exclusions
    source_excludes = exclude_config.get("source_exclude", {}).get(source_name, [])
    for pattern in source_excludes:
        if fnmatch.fnmatch(filename, pattern):
            return True

    return False


def find_lakeflow_connect_class(source_content: str, source_name: str) -> str:
    """
    Find the LakeflowConnect implementation class name in the source file.

    Source connectors must define a class that inherits from LakeflowConnect:
        class MyConnectorLakeflowConnect(LakeflowConnect):
            ...

    Args:
        source_content: The content of the source connector file.
        source_name: The name of the source (for error messages).

    Returns:
        The name of the LakeflowConnect implementation class.

    Raises:
        ValueError: If no LakeflowConnect implementation is found.
        ValueError: If multiple LakeflowConnect implementations are found.
    """
    # Pattern: class SomeName(LakeflowConnect):
    # Matches classes that inherit from LakeflowConnect
    subclass_pattern = r"^class\s+(\w+)\s*\(\s*LakeflowConnect\s*\)\s*:"
    matches = re.findall(subclass_pattern, source_content, re.MULTILINE)

    if len(matches) == 0:
        raise ValueError(
            f"No LakeflowConnect implementation found in {source_name}.py. "
            f"Expected a class definition like: class MyLakeflowConnect(LakeflowConnect):"
        )

    if len(matches) > 1:
        raise ValueError(
            f"Multiple LakeflowConnect implementations found in {source_name}.py: {matches}. "
            f"Expected exactly one class that inherits from LakeflowConnect."
        )

    return matches[0]


def get_all_sources() -> List[str]:
    """
    Discover all available source connectors.
    
    Returns a list of source names that have a {source_name}.py file
    in their directory (excluding 'interface').
    """
    sources_dir = PROJECT_ROOT / "src" / "databricks" / "labs" / "community_connector" / "sources"
    sources = []
    
    for source_dir in sources_dir.iterdir():
        if source_dir.is_dir() and source_dir.name != "interface":
            source_file = source_dir / f"{source_dir.name}.py"
            if source_file.exists():
                sources.append(source_dir.name)
    
    return sorted(sources)


def get_source_lib_files(source_name: str) -> List[Path]:
    """
    Discover additional Python library files in a source directory.

    These are Python files other than __init__.py, the main source file,
    generated files (_generated_*), and files matching exclusion patterns
    from merge_exclude_config.json.

    Recursively searches subdirectories to support organized code structures
    like handlers/, utils/, etc.

    Args:
        source_name: Name of the source (e.g., "github")

    Returns:
        List of Path objects for library files, ordered by import dependencies.
    """
    source_dir = (
        PROJECT_ROOT / "src" / "databricks" / "labs" / "community_connector" 
        / "sources" / source_name
    )

    # Load exclusion configuration
    exclude_config = load_exclude_config()

    lib_files = []
    main_file = f"{source_name}.py"

    # Directories to skip (build artifacts, virtual environments, etc.)
    skip_dirs = {"build", "dist", ".venv", "venv", "__pycache__", ".eggs"}

    # Recursively find all Python files (**.py pattern)
    for py_file in source_dir.rglob("*.py"):
        # Skip files in build artifact directories
        rel_parts = py_file.relative_to(source_dir).parts
        if any(part in skip_dirs or part.endswith(".egg-info") for part in rel_parts):
            continue

        filename = py_file.name
        # Skip main source file, __init__.py, and generated files
        if (filename == main_file 
            or filename == "__init__.py" 
            or filename.startswith("_generated_")):
            continue
        # Skip files matching exclusion patterns
        if should_exclude_file(filename, source_name, exclude_config):
            continue
        lib_files.append(py_file)
    
    if not lib_files:
        return []
    
    # Order files by import dependencies using topological sort
    return order_by_dependencies(lib_files, source_name, source_dir)


def order_by_dependencies(files: List[Path], source_name: str, source_dir: Path) -> List[Path]:
    """
    Order files by their import dependencies (topological sort).

    Files that are imported by others come first. This ensures that when
    merged, all dependencies are defined before they are used.

    Args:
        files: List of Python file paths to order.
        source_name: Name of the source (for import pattern matching).
        source_dir: Path to the source directory.

    Returns:
        List of files in dependency order (dependencies first).
    """
    # Build a map of module path -> file path
    # Module path includes subdirectory (e.g., "handlers.base" for handlers/base.py)
    module_to_file = {}
    for f in files:
        # Get relative path from source_dir and convert to module notation
        rel_path = f.relative_to(source_dir)
        # Convert path to module: handlers/base.py -> handlers.base
        module_path = str(rel_path.with_suffix("")).replace("/", ".").replace("\\", ".")
        module_to_file[module_path] = f
        # Also add just the stem for simple imports (e.g., "zoho_types")
        module_to_file[f.stem] = f
    
    # Build dependency graph: file -> set of files it depends on
    dependencies = {f: set() for f in files}
    
    # Pattern to match imports from this source's modules
    # Matches both flat and nested imports:
    # - "from databricks.labs.community_connector.sources.zoho_crm.zoho_types import ..."
    # - "from databricks.labs.community_connector.sources.zoho_crm.handlers.base import ..."
    import_pattern = re.compile(
        rf"from\s+databricks\.labs\.community_connector\.sources\.{source_name}\.([\w.]+)"
    )
    # Also match relative imports like "from .base import TableHandler" or "from ..zoho_types import ..."
    relative_import_pattern = re.compile(r"from\s+\.+([\w.]+)\s+import")
    
    for f in files:
        content = read_file_content(f)

        # Check absolute imports
        matches = import_pattern.findall(content)
        for module_path in matches:
            # module_path could be "zoho_types" or "handlers.base"
            if module_path in module_to_file:
                dep_file = module_to_file[module_path]
                if dep_file != f:  # Don't add self-dependency
                    dependencies[f].add(dep_file)

        # Check relative imports
        rel_matches = relative_import_pattern.findall(content)
        for rel_module in rel_matches:
            # Try to resolve relative import
            if rel_module in module_to_file:
                dep_file = module_to_file[rel_module]
                if dep_file != f:
                    dependencies[f].add(dep_file)
    
    # Topological sort using Kahn's algorithm
    # Calculate in-degree (number of files that depend on each file)
    in_degree = {f: 0 for f in files}
    for f, deps in dependencies.items():
        for dep in deps:
            # dep is depended upon by f, so dep should come first
            pass  # We need reverse: who depends on whom
    
    # Reverse the graph: file -> set of files that depend on it
    depended_by = {f: set() for f in files}
    for f, deps in dependencies.items():
        for dep in deps:
            depended_by[dep].add(f)
    
    # Calculate in-degree based on dependencies
    in_degree = {f: len(deps) for f, deps in dependencies.items()}
    
    # Start with files that have no dependencies
    queue = [f for f in files if in_degree[f] == 0]
    result = []
    
    while queue:
        # Sort queue for deterministic output
        queue.sort(key=lambda x: x.name)
        current = queue.pop(0)
        result.append(current)
        
        # Reduce in-degree for files that depend on current
        for dependent in depended_by[current]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
    
    # Check for cycles
    if len(result) != len(files):
        remaining = [f.name for f in files if f not in result]
        raise ValueError(
            f"Circular import dependency detected in source '{source_name}' "
            f"involving files: {remaining}"
        )
    
    return result


def read_file_content(file_path: Path) -> str:
    """Read and return the content of a file."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "r") as f:
        return f.read()


def extract_imports_and_code(content: str) -> tuple:
    """
    Extract import statements and remaining code from content.

    Returns:
        Tuple of (list of import lines, remaining code)
    """
    lines = content.split("\n")
    import_lines = []
    code_lines = []
    in_imports = True

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Skip docstrings and comments at the beginning of the file.
        # We only treat them specially while we are still in the initial
        # "import section" (in_imports == True). Once we have seen real
        # code, docstrings are kept as-is.
        if stripped.startswith('"""') or stripped.startswith("'''"):
            quote = '"""' if stripped.startswith('"""') else "'''"
            if in_imports:
                # Skip leading module docstring entirely (may be multi-line)
                # Check if docstring closes on the same line (after the opening quotes)
                rest_of_line = stripped[3:]
                if quote in rest_of_line:
                    # Single-line docstring like """Some text"""
                    i += 1
                    continue
                # Multi-line docstring: skip until we find closing quotes
                i += 1
                while i < len(lines):
                    if quote in lines[i]:
                        i += 1
                        break
                    i += 1
                continue
            else:
                # Inner/function/class docstrings are part of the code
                code_lines.append(line)
                i += 1
                continue

        # Check if this is a *top-level* import line.
        # We deliberately:
        #   - only look for imports while in_imports is True, so imports
        #     inside functions/methods remain in the code section, and
        #   - require the line to be unindented (start at column 0),
        #     so that docstring lines such as "    from the provided ..."
        #     are not mis‑classified as imports.
        if in_imports and line.startswith(("import ", "from ")):
            # Handle multiline imports
            full_import = line
            # Count parentheses in the full import so far
            open_parens = full_import.count("(")
            close_parens = full_import.count(")")

            # Continue if line ends with backslash or has unclosed parentheses
            while line.rstrip().endswith("\\") or (open_parens > close_parens):
                i += 1
                if i < len(lines):
                    line = lines[i]
                    full_import += "\n" + line
                    open_parens = full_import.count("(")
                    close_parens = full_import.count(")")
                else:
                    break
            import_lines.append(full_import)
            in_imports = True
        elif stripped and not stripped.startswith("#"):
            # Non-empty, non-comment line that's not an import
            in_imports = False
            code_lines.append(line)
        else:
            # Empty line or comment
            if in_imports and not import_lines:
                # Skip leading empty lines/comments before any imports
                pass
            elif in_imports and stripped.startswith("#"):
                # Skip comments in import section
                pass
            else:
                # Keep empty lines and comments in code section
                if not in_imports:
                    code_lines.append(line)

        i += 1

    return import_lines, "\n".join(code_lines)


def deduplicate_imports(import_lists: List[List[str]]) -> List[str]:
    """
    Deduplicate and merge imports from multiple sources.

    Merges imports from the same module (e.g., 'from x import a' and 'from x import b'
    becomes 'from x import a, b') and sorts by module name.
    """
    # Imports to skip (internal imports that won't work in merged file)
    skip_patterns = [
        "from databricks.labs.community_connector.libs.utils import",
        "from databricks.labs.community_connector.sparkpds.lakeflow_datasource import",
        "from databricks.labs.community_connector.sources.",
        "from databricks.labs.community_connector.interface",
    ]

    # Track 'from X import Y' style imports to merge them
    from_imports = {}  # module -> set of imported names
    # Track 'import X' style imports and wildcard imports separately
    simple_imports = set()
    wildcard_imports = {}  # module -> full import statement

    for import_list in import_lists:
        for imp in import_list:
            imp_stripped = imp.strip()
            if not imp_stripped:
                continue

            # Check if we should skip this import
            should_skip = any(
                imp_stripped.startswith(pattern) for pattern in skip_patterns
            )
            if should_skip:
                continue

            # Handle 'from X import Y' style imports
            if imp_stripped.startswith("from "):
                # Parse the import statement
                # Handle multiline imports by joining them
                imp_normalized = " ".join(imp_stripped.split())

                if " import " in imp_normalized:
                    parts = imp_normalized.split(" import ", 1)
                    module = parts[0].replace("from ", "").strip()
                    imports_part = parts[1].strip()

                    # Handle wildcard imports specially
                    if imports_part == "*":
                        wildcard_imports[module] = imp_stripped
                    # Handle parenthesized imports
                    elif imports_part.startswith("(") and imports_part.endswith(")"):
                        imports_part = imports_part[1:-1].strip()
                        imported_names = [
                            name.strip()
                            for name in imports_part.split(",")
                            if name.strip()
                        ]
                        if module not in from_imports:
                            from_imports[module] = set()
                        from_imports[module].update(imported_names)
                    else:
                        # Handle comma-separated imports
                        imported_names = [
                            name.strip()
                            for name in imports_part.split(",")
                            if name.strip()
                        ]
                        if module not in from_imports:
                            from_imports[module] = set()
                        from_imports[module].update(imported_names)
            # Handle 'import X' style imports
            else:
                simple_imports.add(imp_stripped)

    # Categorize imports into standard library and third-party
    stdlib_modules = {
        "sys",
        "os",
        "re",
        "json",
        "typing",
        "pathlib",
        "argparse",
        "datetime",
        "time",
        "collections",
        "itertools",
        "functools",
        "dataclasses",
        "enum",
        "abc",
        "io",
        "copy",
        "pickle",
        "decimal",
    }

    def get_base_module(module_name):
        """Extract the base module name from a dotted module path."""
        return module_name.split(".")[0]

    def is_stdlib(module_name):
        """Check if a module is from the standard library."""
        return get_base_module(module_name) in stdlib_modules

    # Build the final import list
    stdlib_from_imports = []
    thirdparty_from_imports = []
    stdlib_simple_imports = []
    thirdparty_simple_imports = []

    # Process 'from X import Y' imports
    for module in sorted(from_imports.keys()):
        # Skip if there's a wildcard import for this module
        if module in wildcard_imports:
            continue

        imported_names = sorted(from_imports[module])
        # Format the import statement
        if len(imported_names) == 1:
            import_stmt = f"from {module} import {imported_names[0]}"
        elif len(imported_names) <= 3:
            # Short list: single line
            import_stmt = f"from {module} import {', '.join(imported_names)}"
        else:
            # Long list: use parentheses and multiple lines for readability
            import_stmt = (
                f"from {module} import (\n    "
                + ",\n    ".join(imported_names)
                + ",\n)"
            )

        if is_stdlib(module):
            stdlib_from_imports.append(import_stmt)
        else:
            thirdparty_from_imports.append(import_stmt)

    # Add wildcard imports
    for module in sorted(wildcard_imports.keys()):
        if is_stdlib(module):
            stdlib_from_imports.append(wildcard_imports[module])
        else:
            thirdparty_from_imports.append(wildcard_imports[module])

    # Process 'import X' imports
    for imp in sorted(simple_imports):
        module = imp.split()[1].split(".")[0]
        if is_stdlib(module):
            stdlib_simple_imports.append(imp)
        else:
            thirdparty_simple_imports.append(imp)

    # Combine all imports in the correct order
    result = []

    # Standard library imports
    if stdlib_from_imports or stdlib_simple_imports:
        result.extend(stdlib_from_imports)
        result.extend(stdlib_simple_imports)

    # Empty line between stdlib and third-party
    if (stdlib_from_imports or stdlib_simple_imports) and (
        thirdparty_from_imports or thirdparty_simple_imports
    ):
        result.append("")

    # Third-party imports
    if thirdparty_from_imports or thirdparty_simple_imports:
        result.extend(thirdparty_from_imports)
        result.extend(thirdparty_simple_imports)

    return result


def merge_files(source_name: str, output_path: Optional[Path] = None) -> str:
    """
    Merge source files into a single file.

    Args:
        source_name: Name of the source (e.g., "zendesk", "example")
        output_path: Optional output file path. If None, saves to sources/{source_name}/_generated_{source_name}_python_source.py

    Returns:
        The merged content as a string
    """
    # Define file paths
    src_base = PROJECT_ROOT / "src" / "databricks" / "labs" / "community_connector"
    utils_path = src_base / "libs" / "utils.py"
    interface_path = src_base / "interface" / "lakeflow_connect.py"
    source_path = src_base / "sources" / source_name / f"{source_name}.py"
    lakeflow_source_path = src_base / "sparkpds" / "lakeflow_datasource.py"

    # If no output path specified, use default location in source directory
    if output_path is None:
        output_path = (
            src_base / "sources" / source_name / f"_generated_{source_name}_python_source.py"
        )

    # Discover additional library files in the source directory
    lib_files = get_source_lib_files(source_name)

    # Verify all files exist
    print(f"Merging files for source: {source_name}", file=sys.stderr)
    print(f"- utils.py: {utils_path}", file=sys.stderr)
    print(f"- lakeflow_connect.py: {interface_path}", file=sys.stderr)
    if lib_files:
        for lib_file in lib_files:
            print(f"- {lib_file.name}: {lib_file}", file=sys.stderr)
    print(f"- {source_name}.py: {source_path}", file=sys.stderr)
    print(f"- lakeflow_datasource.py: {lakeflow_source_path}", file=sys.stderr)

    try:
        # Read all files
        utils_content = read_file_content(utils_path)
        interface_content = read_file_content(interface_path)
        source_content = read_file_content(source_path)
        lakeflow_source_content = read_file_content(lakeflow_source_path)
        
        # Read library files
        lib_contents = []
        for lib_file in lib_files:
            lib_contents.append((lib_file, read_file_content(lib_file)))
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Find the LakeflowConnect implementation class name in the source
    lakeflow_connect_class = find_lakeflow_connect_class(source_content, source_name)
    print(f"- LakeflowConnect implementation: {lakeflow_connect_class}", file=sys.stderr)

    # Extract imports and code from each file
    utils_imports, utils_code = extract_imports_and_code(utils_content)
    interface_imports, interface_code = extract_imports_and_code(interface_content)
    source_imports, source_code = extract_imports_and_code(source_content)
    lakeflow_imports, lakeflow_code = extract_imports_and_code(lakeflow_source_content)
    
    # Extract imports and code from library files
    lib_imports_and_code = []
    for lib_file, content in lib_contents:
        lib_imports, lib_code = extract_imports_and_code(content)
        lib_imports_and_code.append((lib_file, lib_imports, lib_code))

    # Replace the LakeflowConnectImpl alias with the actual implementation class.
    # The placeholder line in lakeflow_datasource.py is:
    #   LakeflowConnectImpl = LakeflowConnect  # __LAKEFLOW_CONNECT_IMPL__
    # We replace it with:
    #   LakeflowConnectImpl = ActualClassName
    placeholder_pattern = (
        r"LakeflowConnectImpl\s*=\s*LakeflowConnect\s*#\s*__LAKEFLOW_CONNECT_IMPL__"
    )
    replacement = f"LakeflowConnectImpl = {lakeflow_connect_class}"
    lakeflow_code, num_replacements = re.subn(placeholder_pattern, replacement, lakeflow_code)

    if num_replacements == 0:
        raise ValueError(
            "Failed to find the LakeflowConnectImpl placeholder in lakeflow_datasource.py. "
            "Expected line: LakeflowConnectImpl = LakeflowConnect  # __LAKEFLOW_CONNECT_IMPL__"
        )
    if num_replacements > 1:
        raise ValueError(
            f"Found {num_replacements} LakeflowConnectImpl placeholders in lakeflow_datasource.py. "
            "Expected exactly one placeholder."
        )

    # Remove # fmt: off and # fmt: on comments (and any immediately following empty lines)
    # These are only needed in the source file to prevent formatter issues
    lakeflow_code_lines = lakeflow_code.split("\n")
    filtered_lines = []
    skip_next_empty = False
    for line in lakeflow_code_lines:
        if line.strip() in ("# fmt: off", "# fmt: on"):
            skip_next_empty = True
            continue
        if skip_next_empty and not line.strip():
            skip_next_empty = False
            continue
        skip_next_empty = False
        filtered_lines.append(line)
    lakeflow_code = "\n".join(filtered_lines)

    # Deduplicate and organize all imports
    all_import_lists = [utils_imports, interface_imports]
    for _, lib_imports, _ in lib_imports_and_code:
        all_import_lists.append(lib_imports)
    all_import_lists.extend([source_imports, lakeflow_imports])
    all_imports = deduplicate_imports(all_import_lists)

    # Build the merged content
    merged_lines = []

    # Header
    merged_lines.append("# " + "=" * 78)
    merged_lines.append(f"# Merged Lakeflow Source: {source_name}")
    merged_lines.append("# " + "=" * 78)
    merged_lines.append(
        "# This file is auto-generated by tools/scripts/merge_python_source.py"
    )
    merged_lines.append(
        "# Do not edit manually. Make changes to the source files instead."
    )
    merged_lines.append("# " + "=" * 78)
    merged_lines.append("")

    # All imports at the top
    if all_imports:
        for imp in all_imports:
            merged_lines.append(imp)
        merged_lines.append("")
        merged_lines.append("")

    # Start the register_lakeflow_source function
    merged_lines.append("def register_lakeflow_source(spark):")
    merged_lines.append('    """Register the Lakeflow Python source with Spark."""')
    merged_lines.append("")

    # Section 1: src/databricks/labs/community_connector/libs/utils.py code
    merged_lines.append("    " + "#" * 56)
    merged_lines.append("    # src/databricks/labs/community_connector/libs/utils.py")
    merged_lines.append("    " + "#" * 56)
    merged_lines.append("")
    # Indent the code
    for line in utils_code.strip().split("\n"):
        if line.strip():  # Only indent non-empty lines
            merged_lines.append("    " + line)
        else:
            merged_lines.append("")
    merged_lines.append("")
    merged_lines.append("")

    # Section 2: src/databricks/labs/community_connector/interface/lakeflow_connect.py code
    merged_lines.append("    " + "#" * 56)
    merged_lines.append(
        "    # src/databricks/labs/community_connector/interface/lakeflow_connect.py"
    )
    merged_lines.append("    " + "#" * 56)
    merged_lines.append("")
    for line in interface_code.strip().split("\n"):
        if line.strip():
            merged_lines.append("    " + line)
        else:
            merged_lines.append("")
    merged_lines.append("")
    merged_lines.append("")

    # Section 3+: Source library files (in dependency order)
    section_num = 3
    for lib_file, _, lib_code in lib_imports_and_code:
        rel_path = lib_file.relative_to(PROJECT_ROOT / "src")
        merged_lines.append("    " + "#" * 56)
        merged_lines.append(f"    # src/{rel_path}")
        merged_lines.append("    " + "#" * 56)
        merged_lines.append("")
        for line in lib_code.strip().split("\n"):
            if line.strip():
                merged_lines.append("    " + line)
            else:
                merged_lines.append("")
        merged_lines.append("")
        merged_lines.append("")
        section_num += 1

    # Main source file: src/databricks/labs/community_connector/sources/{source_name}/{source_name}.py code
    merged_lines.append("    " + "#" * 56)
    merged_lines.append(
        f"    # src/databricks/labs/community_connector/sources/{source_name}/{source_name}.py"
    )
    merged_lines.append("    " + "#" * 56)
    merged_lines.append("")
    for line in source_code.strip().split("\n"):
        if line.strip():
            merged_lines.append("    " + line)
        else:
            merged_lines.append("")
    merged_lines.append("")
    merged_lines.append("")

    # Final section: Spark DataSource registration
    merged_lines.append("    " + "#" * 56)
    merged_lines.append(
        "    # src/databricks/labs/community_connector/sparkpds/lakeflow_datasource.py"
    )
    merged_lines.append("    " + "#" * 56)
    merged_lines.append("")
    for line in lakeflow_code.strip().split("\n"):
        if line.strip():
            merged_lines.append("    " + line)
        else:
            merged_lines.append("")
    merged_lines.append("")

    # Register the data source with Spark
    merged_lines.append("\n    spark.dataSource.register(LakeflowSource)")
    merged_lines.append("")

    merged_content = "\n".join(merged_lines)

    # Write to output file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(merged_content)
    print(f"\nMerged file written to: {output_path}", file=sys.stderr)

    return merged_content


def merge_all_sources() -> None:
    """Merge all available source connectors."""
    sources = get_all_sources()
    print(f"Regenerating {len(sources)} sources: {', '.join(sources)}", file=sys.stderr)
    print(
        "Output: src/databricks/labs/community_connector/sources/<source>/_generated_<source>_python_source.py",
        file=sys.stderr,
    )
    print("", file=sys.stderr)
    
    for source in sources:
        try:
            merge_files(source)
        except Exception as e:
            print(f"Error merging {source}: {e}", file=sys.stderr)
            sys.exit(1)
    
    print("", file=sys.stderr)
    print(f"✅ All {len(sources)} sources regenerated!", file=sys.stderr)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Merge source files into a single deployable file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Merge zendesk source (saves to src/databricks/labs/community_connector/sources/zendesk/_generated_zendesk_python_source.py)
  python tools/scripts/merge_python_source.py zendesk

  # Merge example source (saves to src/databricks/labs/community_connector/sources/example/_generated_example_python_source.py)
  python tools/scripts/merge_python_source.py example

  # Merge zendesk source and save to custom location
  python tools/scripts/merge_python_source.py zendesk -o output/zendesk_merged.py

  # Regenerate all sources
  python tools/scripts/merge_python_source.py all
        """,
    )

    parser.add_argument(
        "source_name", 
        help="Name of the source to merge (e.g., zendesk, example) or 'all' to regenerate all sources"
    )

    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output file path (default: src/databricks/labs/community_connector/sources/{source_name}/_generated_{source_name}_python_source.py). Not applicable when using 'all'.",
    )

    args = parser.parse_args()

    try:
        if args.source_name == "all":
            if args.output:
                print("Warning: --output is ignored when using 'all'", file=sys.stderr)
            merge_all_sources()
        else:
            merge_files(args.source_name, args.output)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
