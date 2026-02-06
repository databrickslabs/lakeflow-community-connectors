"""
Connector spec parsing and validation utilities.

This module handles parsing connector_spec.yaml files and validating
connection options against the spec.

Supports two connection parameter structures:
- Option A: Flat 'parameters' list (single auth method)
- Option B: 'auth_methods' with 'parameters' for shared params (multiple auth methods)
"""

import urllib.request
import urllib.error
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple, List, Set, Callable

import yaml


@dataclass
class AuthMethod:
    """Represents an authentication method from the connector spec."""

    name: str
    description: str
    required_params: Set[str] = field(default_factory=set)
    optional_params: Set[str] = field(default_factory=set)


@dataclass
class ParsedConnectorSpec:
    """Parsed connector spec with support for both flat parameters and auth_methods."""

    # For Option A (flat parameters) - these are used directly
    required_params: Set[str] = field(default_factory=set)
    optional_params: Set[str] = field(default_factory=set)

    # For Option B (auth_methods)
    auth_methods: List[AuthMethod] = field(default_factory=list)
    common_required_params: Set[str] = field(default_factory=set)
    common_optional_params: Set[str] = field(default_factory=set)

    external_options_allowlist: str = ""

    def has_auth_methods(self) -> bool:
        """Check if this spec uses auth_methods (Option B)."""
        return len(self.auth_methods) > 0

    def get_all_known_params(self) -> Set[str]:
        """Get all known parameter names across all auth methods and common params."""
        all_params = self.required_params | self.optional_params
        all_params |= self.common_required_params | self.common_optional_params
        for auth_method in self.auth_methods:
            all_params |= auth_method.required_params | auth_method.optional_params
        return all_params


@dataclass
class ValidationResult:
    """Result of connection options validation."""

    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    detected_auth_method: Optional[str] = None

    def is_valid(self) -> bool:
        """Return True if validation passed (no errors)."""
        return len(self.errors) == 0


def convert_github_url_to_raw(url: str, branch: str = "master") -> str:
    """
    Convert a GitHub repository URL to a raw.githubusercontent.com URL.

    Args:
        url: GitHub URL (e.g., https://github.com/org/repo or https://github.com/org/repo.git)
        branch: Branch name to use (default: master)

    Returns:
        Raw GitHub URL (e.g., https://raw.githubusercontent.com/org/repo/master)
    """
    # Remove trailing slash and .git suffix
    url = url.rstrip("/")
    if url.endswith(".git"):
        url = url[:-4]

    # Handle different GitHub URL formats
    if "github.com" in url:
        # Extract org/repo from URL
        # Handles: https://github.com/org/repo, git@github.com:org/repo, etc.
        if url.startswith("https://github.com/"):
            path = url.replace("https://github.com/", "")
        elif url.startswith("http://github.com/"):
            path = url.replace("http://github.com/", "")
        elif url.startswith("git@github.com:"):
            path = url.replace("git@github.com:", "")
        else:
            # Unknown format, return as-is
            return url

        return f"https://raw.githubusercontent.com/{path}/{branch}"

    # If already a raw URL or unknown format, return as-is
    return url


def _load_yaml_file(path: Path) -> Optional[dict]:
    """Load a YAML file from disk, returning None on any error."""
    if not path.exists():
        return None
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return None


def load_connector_spec(
    source_name: str,
    spec_path: Optional[str] = None,
    get_default_repo_url: Optional[Callable[[], str]] = None,
    cli_file_path: Optional[str] = None,
    warn_callback: Optional[Callable[[str], None]] = None,
) -> Optional[dict]:
    """
    Load the connector_spec.yaml for a given source.

    Priority:
    1. If spec_path is a local file path, use it directly
    2. Try local sources directory (for development within the repo)
    3. If spec_path is a URL (repo URL), convert to raw URL and fetch from that repo
    4. Fall back to fetching from the default GitHub repo

    Args:
        source_name: Name of the connector source (e.g., 'github', 'stripe').
        spec_path: Optional path to a local spec file, or a GitHub repo URL.
        get_default_repo_url: Callback to get the default repo raw URL.
        cli_file_path: Path to the CLI file for resolving relative paths.
        warn_callback: Callback for warning messages.

    Returns:
        Parsed connector spec as a dictionary, or None if not found.
    """

    def warn(msg: str) -> None:
        if warn_callback:
            warn_callback(msg)

    # 1. If spec_path is provided and is a local file, use it directly
    if spec_path and not spec_path.startswith(("http://", "https://")):
        result = _load_yaml_file(Path(spec_path))
        if result is None:
            warn(f"Spec file not found or could not be read: {spec_path}")
        return result

    # 2. Try local paths (for development within the repo)
    local_paths = []
    if cli_file_path:
        # Development path - relative to cli.py location (8 levels up to repo root)
        cli_parent = Path(cli_file_path).parent
        local_paths.append(
            cli_parent.parent.parent.parent.parent.parent.parent.parent
            / "src"
            / "databricks"
            / "labs"
            / "community_connector_cli"
            / "sources"
            / source_name
            / "connector_spec.yaml"
        )

    # Current working directory is repo root
    local_paths.append(
        Path.cwd()
        / "src"
        / "databricks"
        / "labs"
        / "community_connector_cli"
        / "sources"
        / source_name
        / "connector_spec.yaml"
    )
    # Current working directory is tools/community_connector (2 levels up to repo root)
    local_paths.append(
        Path.cwd().parent.parent
        / "src"
        / "databricks"
        / "labs"
        / "community_connector_cli"
        / "sources"
        / source_name
        / "connector_spec.yaml"
    )

    for local_path in local_paths:
        result = _load_yaml_file(local_path)
        if result is not None:
            return result

    # 3. Determine the repo URL to use (custom URL or default from config)
    if spec_path:
        # Convert GitHub URL to raw URL (assumes master branch for custom repos)
        repo_raw_url = convert_github_url_to_raw(spec_path)
    elif get_default_repo_url:
        repo_raw_url = get_default_repo_url()
    else:
        repo_raw_url = (
            "https://raw.githubusercontent.com/databrickslabs/lakeflow-community-connectors/master"
        )

    # Remove trailing slash if present
    repo_raw_url = repo_raw_url.rstrip("/")

    spec_url = f"{repo_raw_url}/src/databricks/labs/community_connector/sources/{source_name}/connector_spec.yaml"
    try:
        with urllib.request.urlopen(spec_url, timeout=10) as response:
            content = response.read().decode("utf-8")
            return yaml.safe_load(content)
    except Exception as e:
        warn(f"Could not fetch connector spec from: {spec_url} ({e})")
        return None


def parse_parameters(parameters: list) -> Tuple[Set[str], Set[str]]:
    """
    Parse a list of parameter definitions into required and optional sets.

    Args:
        parameters: List of parameter dictionaries.

    Returns:
        Tuple of (required_params, optional_params).
    """
    required_params: Set[str] = set()
    optional_params: Set[str] = set()

    for param in parameters:
        if isinstance(param, dict):
            name = param.get("name")
            if name:
                if param.get("required", False):
                    required_params.add(name)
                else:
                    optional_params.add(name)

    return required_params, optional_params


def parse_connector_spec(spec: dict) -> ParsedConnectorSpec:
    """
    Parse the connector spec to extract parameters and external options allowlist.

    Supports two structures:
    - Option A: Flat 'parameters' list under 'connection'
    - Option B: 'auth_methods' with 'parameters' for shared params under 'connection'

    Args:
        spec: Parsed connector spec dictionary.

    Returns:
        ParsedConnectorSpec object with all extracted information.
    """
    parsed = ParsedConnectorSpec()

    connection = spec.get("connection", {})

    # Check for Option B: auth_methods structure
    auth_methods = connection.get("auth_methods", [])
    if auth_methods:
        for method in auth_methods:
            if isinstance(method, dict):
                method_params = method.get("parameters", [])
                required, optional = parse_parameters(method_params)
                parsed.auth_methods.append(
                    AuthMethod(
                        name=method.get("name", ""),
                        description=method.get("description", ""),
                        required_params=required,
                        optional_params=optional,
                    )
                )

        # Parse shared parameters (previously called common_parameters)
        common_params = connection.get("parameters", [])
        parsed.common_required_params, parsed.common_optional_params = parse_parameters(
            common_params
        )
    else:
        # Option A: flat parameters structure
        parameters = connection.get("parameters", [])
        parsed.required_params, parsed.optional_params = parse_parameters(parameters)

    # Parse external options allowlist
    external_options_allowlist = spec.get("external_options_allowlist", "")
    if external_options_allowlist is None:
        external_options_allowlist = ""
    parsed.external_options_allowlist = external_options_allowlist

    return parsed


def parse_connector_spec_legacy(spec: dict) -> Tuple[Set[str], Set[str], str]:
    """
    Legacy wrapper for parse_connector_spec that returns the old tuple format.

    This is for backward compatibility with existing code that expects:
    Tuple of (required_params, optional_params, external_options_allowlist).

    For specs with auth_methods, this returns all params as optional since
    the actual validation is done by validate_connection_options.
    """
    parsed = parse_connector_spec(spec)

    if parsed.has_auth_methods():
        # For auth_methods, we treat auth-specific params as optional at this level
        # The actual auth method validation happens in validate_connection_options
        all_auth_params: Set[str] = set()
        for method in parsed.auth_methods:
            all_auth_params |= method.required_params | method.optional_params

        return (
            parsed.common_required_params,
            parsed.common_optional_params | all_auth_params,
            parsed.external_options_allowlist,
        )
    else:
        return (
            parsed.required_params,
            parsed.optional_params,
            parsed.external_options_allowlist,
        )


def merge_external_options_allowlist(source_allowlist: str, constant_allowlist: str) -> str:
    """
    Merge the source-specific allowlist with the constant allowlist.

    Combines both allowlists, removes duplicates, and returns a comma-separated string.

    Args:
        source_allowlist: Comma-separated allowlist from the connector spec.
        constant_allowlist: Comma-separated constant allowlist from config.

    Returns:
        Merged comma-separated allowlist string.
    """
    # Parse both allowlists into sets, filtering out empty strings
    source_items = {item.strip() for item in source_allowlist.split(",") if item.strip()}
    constant_items = {item.strip() for item in constant_allowlist.split(",") if item.strip()}

    # Merge and sort for consistent output
    merged = source_items | constant_items

    return ",".join(sorted(merged))


def detect_auth_method(
    options_dict: dict, parsed_spec: ParsedConnectorSpec
) -> Optional[AuthMethod]:
    """
    Detect which auth method the user is trying to use based on provided options.

    Args:
        options_dict: The connection options dictionary.
        parsed_spec: The parsed connector spec.

    Returns:
        The detected AuthMethod, or None if no match found.
    """
    if not parsed_spec.has_auth_methods():
        return None

    provided_params = set(options_dict.keys())
    best_match: Optional[AuthMethod] = None
    best_match_score = 0

    for method in parsed_spec.auth_methods:
        # Calculate how many of this method's required params are provided
        method_params = method.required_params | method.optional_params
        overlap = len(provided_params & method_params)

        # Check if all required params for this method are provided
        has_all_required = method.required_params.issubset(provided_params)

        if has_all_required and overlap > best_match_score:
            best_match = method
            best_match_score = overlap

    return best_match


def validate_connection_options(
    source_name: str,
    options_dict: dict,
    parsed_spec: ParsedConnectorSpec,
) -> ValidationResult:
    """
    Validate connection options against the parsed connector spec.

    Supports both flat parameters and auth_methods structures.

    Args:
        source_name: Name of the connector source.
        options_dict: The connection options dictionary.
        parsed_spec: The parsed connector spec.

    Returns:
        ValidationResult with errors, warnings, and detected auth method.
    """
    result = ValidationResult()
    provided_params = set(options_dict.keys())

    if parsed_spec.has_auth_methods():
        # Option B: auth_methods structure
        # First, check common required parameters
        missing_common = parsed_spec.common_required_params - provided_params
        if missing_common:
            result.errors.append(
                f"Missing required common parameters: {', '.join(sorted(missing_common))}"
            )

        # Detect which auth method is being used
        detected_method = detect_auth_method(options_dict, parsed_spec)

        if detected_method:
            result.detected_auth_method = detected_method.name
            # Validate all required params for the detected method
            missing_method_params = detected_method.required_params - provided_params
            if missing_method_params:
                result.errors.append(
                    f"Missing required parameters for '{detected_method.name}' auth method: "
                    f"{', '.join(sorted(missing_method_params))}"
                )
        else:
            # No auth method detected - show available options
            method_descriptions = []
            for method in parsed_spec.auth_methods:
                required_str = ", ".join(sorted(method.required_params))
                method_descriptions.append(f"  - {method.name}: requires ({required_str})")

            result.errors.append(
                "No valid authentication method detected. "
                "Please provide all required parameters for ONE of the following:\n"
                + "\n".join(method_descriptions)
            )

        # Check for unknown parameters
        all_known = parsed_spec.get_all_known_params()
        all_known.add("sourceName")
        all_known.add("externalOptionsAllowList")
        unknown_params = provided_params - all_known

        if unknown_params:
            result.errors.append(
                f"Unknown connection parameters for '{source_name}': "
                f"{', '.join(sorted(unknown_params))}. "
                f"Known parameters are: {', '.join(sorted(parsed_spec.get_all_known_params()))}"
            )
    else:
        # Option A: flat parameters structure
        missing_required = parsed_spec.required_params - provided_params
        if missing_required:
            result.errors.append(
                f"Missing required connection parameters: {', '.join(sorted(missing_required))}"
            )

        # Check for unknown parameters
        all_known = parsed_spec.required_params | parsed_spec.optional_params
        all_known.add("sourceName")
        all_known.add("externalOptionsAllowList")
        unknown_params = provided_params - all_known

        if unknown_params:
            result.errors.append(
                f"Unknown connection parameters for '{source_name}': "
                f"{', '.join(sorted(unknown_params))}. "
                f"Known parameters are: "
                f"{', '.join(sorted(parsed_spec.required_params | parsed_spec.optional_params))}"
            )

    return result


def validate_connection_options_legacy(
    source_name: str,
    options_dict: dict,
    required_params: Set[str],
    optional_params: Set[str],
) -> ValidationResult:
    """
    Legacy validation function for flat parameters.

    Validate connection options against required and optional parameter sets.

    Args:
        source_name: Name of the connector source.
        options_dict: The connection options dictionary.
        required_params: Set of required parameter names.
        optional_params: Set of optional parameter names.

    Returns:
        ValidationResult with errors and warnings.
    """
    result = ValidationResult()

    # Check all required parameters are provided
    missing_required = required_params - set(options_dict.keys())
    if missing_required:
        result.errors.append(
            f"Missing required connection parameters: {', '.join(sorted(missing_required))}"
        )

    # Check for unknown parameters (not in required or optional)
    all_known_params = required_params | optional_params
    # Also allow 'sourceName' and 'externalOptionsAllowList' which are auto-added
    all_known_params.add("sourceName")
    all_known_params.add("externalOptionsAllowList")

    provided_params = set(options_dict.keys())
    unknown_params = provided_params - all_known_params
    if unknown_params:
        result.errors.append(
            f"Unknown connection parameters for '{source_name}': {', '.join(sorted(unknown_params))}. "
            f"Known parameters are: {', '.join(sorted(required_params | optional_params))}"
        )

    return result
