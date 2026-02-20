"""Interactive CLI tool that reads a connector_spec.yaml and prompts the user
to provide each connection parameter, then writes the result as JSON."""

import argparse
import getpass
import json
import sys
from pathlib import Path

import yaml

SOURCES_DIR = Path("src/databricks/labs/community_connector/sources")
TESTS_DIR = Path("tests/unit/sources")


def find_project_root() -> Path:
    """Walk up from cwd to find the project root (contains pyproject.toml)."""
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return current


def resolve_spec_path(source_name: str, project_root: Path) -> Path:
    spec_path = project_root / SOURCES_DIR / source_name / "connector_spec.yaml"
    if not spec_path.exists():
        print(f"Error: spec file not found: {spec_path}", file=sys.stderr)
        sys.exit(1)
    return spec_path


def load_spec(spec_path: Path) -> dict:
    with open(spec_path) as f:
        return yaml.safe_load(f)


def extract_parameters(spec: dict) -> list[dict]:
    try:
        return spec["connection"]["parameters"]
    except (KeyError, TypeError):
        print("Error: YAML does not contain connection.parameters", file=sys.stderr)
        sys.exit(1)


def prompt_for_parameter(param: dict) -> str | None:
    name = param["name"]
    description = param.get("description", "").strip()
    required = param.get("required", False)
    is_secret = param.get("secret", False)

    tag = "REQUIRED" if required else "optional"
    print(f"\n── {name} ({tag}) ──")
    if description:
        print(f"   {description}")

    prompt_text = f"  {name}: "
    if is_secret:
        value = getpass.getpass(prompt_text)
    else:
        value = input(prompt_text)

    value = value.strip()

    if not value:
        if required:
            print(f"  ⚠  '{name}' is required. Please provide a value.")
            return prompt_for_parameter(param)
        return None

    return value


def run(source_name: str, output_file: Path) -> None:
    project_root = find_project_root()
    spec_path = resolve_spec_path(source_name, project_root)
    spec = load_spec(spec_path)
    display_name = spec.get("display_name", source_name)
    parameters = extract_parameters(spec)

    required_params = [p for p in parameters if p.get("required", False)]
    optional_params = [p for p in parameters if not p.get("required", False)]

    print(f"\n{'='*60}")
    print(f"  Configure connection for: {display_name}")
    print(f"  {len(required_params)} required, {len(optional_params)} optional parameter(s)")
    print(f"{'='*60}")

    collected: dict[str, str] = {}

    if required_params:
        print("\n▸ Required parameters:")
        for param in required_params:
            value = prompt_for_parameter(param)
            if value is not None:
                collected[param["name"]] = value

    if optional_params:
        print("\n▸ Optional parameters (press Enter to skip):")
        for param in optional_params:
            value = prompt_for_parameter(param)
            if value is not None:
                collected[param["name"]] = value

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w") as f:
        json.dump(collected, f, indent=2)
        f.write("\n")

    print(f"\n✓ Configuration saved to {output_file}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Interactively collect connection parameters defined in a connector_spec.yaml and save them as JSON.",
    )
    parser.add_argument(
        "-s",
        "--source",
        required=True,
        help="Source connector name (e.g. zendesk, stripe, github)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output file path (default: tests/unit/sources/<source>/configs/dev_config.json)",
    )
    args = parser.parse_args()

    if args.output:
        output_file = Path(args.output)
    else:
        project_root = find_project_root()
        output_file = project_root / TESTS_DIR / args.source / "configs" / "dev_config.json"

    run(args.source, output_file)


if __name__ == "__main__":
    main()
