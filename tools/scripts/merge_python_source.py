#!/usr/bin/env python3
"""
Script to merge source files into a single deployable file.
Due to current limitations, the Spark Declarative Pipeline (SDP) does not
support module imports for Python Data Source implementations.

This script combines:
1. libs/utils.py (parsing utilities)
2. sources/{source_name}/{source_name}.py (source connector implementation)
3. pipeline/lakeflow_python_source.py (PySpark data source registration)

Usage:
    python tools/scripts/merge_python_source.py <source_name>
    python tools/scripts/merge_python_source.py zendesk
    python tools/scripts/merge_python_source.py zendesk -o output/zendesk_merged.py
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional


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
            if in_imports:
                # Skip leading module docstring lines entirely
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
        "from libs.utils import",
        "from pipeline.lakeflow_python_source import",
        "from sources.",
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
            should_skip = any(imp_stripped.startswith(pattern) for pattern in skip_patterns)
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
                            name.strip() for name in imports_part.split(",") if name.strip()
                        ]
                        if module not in from_imports:
                            from_imports[module] = set()
                        from_imports[module].update(imported_names)
                    else:
                        # Handle comma-separated imports
                        imported_names = [
                            name.strip() for name in imports_part.split(",") if name.strip()
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
            import_stmt = f"from {module} import (\n    " + ",\n    ".join(imported_names) + ",\n)"

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
    Merge the three files into a single file.

    Args:
        source_name: Name of the source (e.g., "zendesk", "example")
        output_path: Optional output file path. If None, saves to sources/{source_name}/_generated_{source_name}_python_source.py

    Returns:
        The merged content as a string
    """
    # Get the project root directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    # Define file paths
    utils_path = project_root / "libs" / "utils.py"
    source_path = project_root / "sources" / source_name / f"{source_name}.py"
    lakeflow_source_path = project_root / "pipeline" / "lakeflow_python_source.py"

    # If no output path specified, use default location in source directory
    if output_path is None:
        output_path = (
            project_root / "sources" / source_name / f"_generated_{source_name}_python_source.py"
        )

    # Verify all files exist
    print(f"Merging files for source: {source_name}", file=sys.stderr)
    print(f"- utils.py: {utils_path}", file=sys.stderr)
    print(f"- {source_name}.py: {source_path}", file=sys.stderr)
    print(f"- lakeflow_python_source.py: {lakeflow_source_path}", file=sys.stderr)

    try:
        # Read all files
        utils_content = read_file_content(utils_path)
        source_content = read_file_content(source_path)
        lakeflow_source_content = read_file_content(lakeflow_source_path)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Extract imports and code from each file
    utils_imports, utils_code = extract_imports_and_code(utils_content)
    source_imports, source_code = extract_imports_and_code(source_content)
    lakeflow_imports, lakeflow_code = extract_imports_and_code(lakeflow_source_content)

    # Deduplicate and organize all imports
    all_imports = deduplicate_imports([utils_imports, source_imports, lakeflow_imports])

    # Build the merged content
    merged_lines = []

    # Header
    merged_lines.append("# " + "=" * 78)
    merged_lines.append(f"# Merged Lakeflow Source: {source_name}")
    merged_lines.append("# " + "=" * 78)
    merged_lines.append("# This file is auto-generated by tools/scripts/merge_python_source.py")
    merged_lines.append("# Do not edit manually. Make changes to the source files instead.")
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

    # Section 1: libs/utils.py code
    merged_lines.append("    " + "#" * 56)
    merged_lines.append("    # libs/utils.py")
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

    # Section 2: sources/{source_name}/{source_name}.py code
    merged_lines.append("    " + "#" * 56)
    merged_lines.append(f"    # sources/{source_name}/{source_name}.py")
    merged_lines.append("    " + "#" * 56)
    merged_lines.append("")
    for line in source_code.strip().split("\n"):
        if line.strip():
            merged_lines.append("    " + line)
        else:
            merged_lines.append("")
    merged_lines.append("")
    merged_lines.append("")

    # Section 3: pipeline/lakeflow_python_source.py code
    merged_lines.append("    " + "#" * 56)
    merged_lines.append("    # pipeline/lakeflow_python_source.py")
    merged_lines.append("    " + "#" * 56)
    merged_lines.append("")
    for line in lakeflow_code.strip().split("\n"):
        if line.strip():
            merged_lines.append("    " + line)
        else:
            merged_lines.append("")
    merged_lines.append("")

    merged_content = "\n".join(merged_lines)

    # Write to output file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(merged_content)
    print(f"\nMerged file written to: {output_path}", file=sys.stderr)

    return merged_content


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Merge source files into a single deployable file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Merge zendesk source (saves to sources/zendesk/_generated_zendesk_python_source.py)
  python tools/scripts/merge_python_source.py zendesk

  # Merge example source (saves to sources/example/_generated_example_python_source.py)
  python tools/scripts/merge_python_source.py example

  # Merge zendesk source and save to custom location
  python tools/scripts/merge_python_source.py zendesk -o output/zendesk_merged.py
        """,
    )

    parser.add_argument("source_name", help="Name of the source to merge (e.g., zendesk, example)")

    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output file path (default: sources/{source_name}/_generated_{source_name}_python_source.py)",
    )

    args = parser.parse_args()

    try:
        merge_files(args.source_name, args.output)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
