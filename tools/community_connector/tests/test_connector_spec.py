"""
Unit tests for the connector_spec module.

Tests connector spec parsing, validation, and helper functions.
"""

import pytest

from databricks.labs.community_connector.connector_spec import (
    AuthMethod,
    ParsedConnectorSpec,
    ValidationResult,
    convert_github_url_to_raw,
    parse_connector_spec,
    parse_connector_spec_legacy,
    parse_parameters,
    merge_external_options_allowlist,
    detect_auth_method,
    validate_connection_options,
    validate_connection_options_legacy,
)


class TestConvertGithubUrlToRaw:
    """Tests for convert_github_url_to_raw function."""

    def test_https_url(self):
        """Test converting HTTPS GitHub URL."""
        url = "https://github.com/databrickslabs/lakeflow-community-connectors"
        result = convert_github_url_to_raw(url)
        assert result == "https://raw.githubusercontent.com/databrickslabs/lakeflow-community-connectors/master"

    def test_https_url_with_git_suffix(self):
        """Test converting HTTPS GitHub URL with .git suffix."""
        url = "https://github.com/databrickslabs/lakeflow-community-connectors.git"
        result = convert_github_url_to_raw(url)
        assert result == "https://raw.githubusercontent.com/databrickslabs/lakeflow-community-connectors/master"

    def test_https_url_with_trailing_slash(self):
        """Test converting HTTPS GitHub URL with trailing slash."""
        url = "https://github.com/databrickslabs/lakeflow-community-connectors/"
        result = convert_github_url_to_raw(url)
        assert result == "https://raw.githubusercontent.com/databrickslabs/lakeflow-community-connectors/master"

    def test_custom_branch(self):
        """Test converting with custom branch."""
        url = "https://github.com/databrickslabs/lakeflow-community-connectors"
        result = convert_github_url_to_raw(url, branch="develop")
        assert result == "https://raw.githubusercontent.com/databrickslabs/lakeflow-community-connectors/develop"

    def test_ssh_url(self):
        """Test converting SSH GitHub URL."""
        url = "git@github.com:databrickslabs/lakeflow-community-connectors"
        result = convert_github_url_to_raw(url)
        assert result == "https://raw.githubusercontent.com/databrickslabs/lakeflow-community-connectors/master"

    def test_already_raw_url(self):
        """Test that raw URLs are returned as-is."""
        url = "https://raw.githubusercontent.com/org/repo/main"
        result = convert_github_url_to_raw(url)
        assert result == url

    def test_non_github_url(self):
        """Test that non-GitHub URLs are returned as-is."""
        url = "https://gitlab.com/org/repo"
        result = convert_github_url_to_raw(url)
        assert result == url


class TestParseParameters:
    """Tests for parse_parameters function."""

    def test_parse_required_and_optional(self):
        """Test parsing parameters with both required and optional."""
        parameters = [
            {"name": "token", "type": "string", "required": True},
            {"name": "base_url", "type": "string", "required": False},
            {"name": "timeout", "type": "integer"},  # defaults to False
        ]
        required, optional = parse_parameters(parameters)
        assert required == {"token"}
        assert optional == {"base_url", "timeout"}

    def test_parse_empty_list(self):
        """Test parsing empty parameter list."""
        required, optional = parse_parameters([])
        assert required == set()
        assert optional == set()

    def test_parse_invalid_entries(self):
        """Test that invalid entries are skipped."""
        parameters = [
            {"name": "valid", "required": True},
            "invalid_string",
            {"no_name": True},
            None,
        ]
        required, optional = parse_parameters(parameters)
        assert required == {"valid"}
        assert optional == set()


class TestParseConnectorSpec:
    """Tests for parse_connector_spec function."""

    def test_parse_spec_with_required_and_optional_params(self):
        """Test parsing a spec with both required and optional parameters (Option A)."""
        spec = {
            "connection": {
                "parameters": [
                    {"name": "token", "type": "string", "required": True},
                    {"name": "base_url", "type": "string", "required": False},
                    {"name": "timeout", "type": "integer", "required": False},
                ]
            },
            "external_options_allowlist": "owner,repo,state",
        }

        parsed = parse_connector_spec(spec)

        assert parsed.required_params == {"token"}
        assert parsed.optional_params == {"base_url", "timeout"}
        assert parsed.external_options_allowlist == "owner,repo,state"
        assert not parsed.has_auth_methods()

    def test_parse_spec_with_empty_parameters(self):
        """Test parsing a spec with no parameters."""
        spec = {
            "connection": {"parameters": []},
            "external_options_allowlist": "",
        }

        parsed = parse_connector_spec(spec)

        assert parsed.required_params == set()
        assert parsed.optional_params == set()
        assert parsed.external_options_allowlist == ""

    def test_parse_spec_with_missing_connection(self):
        """Test parsing a spec with missing connection section."""
        spec = {
            "external_options_allowlist": "option1,option2",
        }

        parsed = parse_connector_spec(spec)

        assert parsed.required_params == set()
        assert parsed.optional_params == set()
        assert parsed.external_options_allowlist == "option1,option2"

    def test_parse_spec_with_none_allowlist(self):
        """Test parsing a spec with None external_options_allowlist."""
        spec = {
            "connection": {"parameters": []},
            "external_options_allowlist": None,
        }

        parsed = parse_connector_spec(spec)

        assert parsed.external_options_allowlist == ""

    def test_parse_spec_with_no_required_field(self):
        """Test parsing a spec where parameters lack required field (defaults to False)."""
        spec = {
            "connection": {
                "parameters": [
                    {"name": "option1", "type": "string"},
                    {"name": "option2", "type": "string", "required": True},
                ]
            },
            "external_options_allowlist": "",
        }

        parsed = parse_connector_spec(spec)

        assert parsed.required_params == {"option2"}
        assert parsed.optional_params == {"option1"}

    def test_parse_spec_with_auth_methods(self):
        """Test parsing a spec with auth_methods structure (Option B)."""
        spec = {
            "connection": {
                "auth_methods": [
                    {
                        "name": "service_account",
                        "description": "Use service account credentials.",
                        "parameters": [
                            {"name": "username", "type": "string", "required": True},
                            {"name": "secret", "type": "string", "required": True},
                        ],
                    },
                    {
                        "name": "api_secret",
                        "description": "Use API secret.",
                        "parameters": [
                            {"name": "api_secret", "type": "string", "required": True},
                        ],
                    },
                ],
                "common_parameters": [
                    {"name": "project_id", "type": "string", "required": False},
                    {"name": "region", "type": "string", "required": False},
                ],
            },
            "external_options_allowlist": "",
        }

        parsed = parse_connector_spec(spec)

        assert parsed.has_auth_methods()
        assert len(parsed.auth_methods) == 2
        assert parsed.auth_methods[0].name == "service_account"
        assert parsed.auth_methods[0].required_params == {"username", "secret"}
        assert parsed.auth_methods[1].name == "api_secret"
        assert parsed.auth_methods[1].required_params == {"api_secret"}
        assert parsed.common_required_params == set()
        assert parsed.common_optional_params == {"project_id", "region"}

    def test_parse_spec_with_auth_methods_and_common_required(self):
        """Test parsing a spec with required common parameters."""
        spec = {
            "connection": {
                "auth_methods": [
                    {
                        "name": "api_token",
                        "description": "Use API token.",
                        "parameters": [
                            {"name": "email", "type": "string", "required": True},
                            {"name": "api_token", "type": "string", "required": True},
                        ],
                    },
                ],
                "common_parameters": [
                    {"name": "subdomain", "type": "string", "required": True},
                ],
            },
            "external_options_allowlist": "filter,page_size",
        }

        parsed = parse_connector_spec(spec)

        assert parsed.has_auth_methods()
        assert parsed.common_required_params == {"subdomain"}
        assert parsed.external_options_allowlist == "filter,page_size"


class TestParsedConnectorSpec:
    """Tests for ParsedConnectorSpec class methods."""

    def test_has_auth_methods_false(self):
        """Test has_auth_methods returns False for flat params."""
        spec = ParsedConnectorSpec(
            required_params={"token"},
            optional_params={"timeout"},
        )
        assert not spec.has_auth_methods()

    def test_has_auth_methods_true(self):
        """Test has_auth_methods returns True when auth_methods exist."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(name="api_key", description="", required_params={"api_key"})
            ]
        )
        assert spec.has_auth_methods()

    def test_get_all_known_params_flat(self):
        """Test get_all_known_params for flat parameters."""
        spec = ParsedConnectorSpec(
            required_params={"token", "api_key"},
            optional_params={"timeout"},
        )
        assert spec.get_all_known_params() == {"token", "api_key", "timeout"}

    def test_get_all_known_params_auth_methods(self):
        """Test get_all_known_params includes all auth method parameters."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(
                    name="method1",
                    description="",
                    required_params={"param1"},
                    optional_params={"opt1"},
                ),
                AuthMethod(
                    name="method2",
                    description="",
                    required_params={"param2"},
                    optional_params=set(),
                ),
            ],
            common_required_params={"common_req"},
            common_optional_params={"common_opt"},
        )
        all_params = spec.get_all_known_params()
        assert all_params == {"param1", "opt1", "param2", "common_req", "common_opt"}


class TestParseConnectorSpecLegacy:
    """Tests for parse_connector_spec_legacy function."""

    def test_legacy_flat_params(self):
        """Test legacy format for flat parameters."""
        spec = {
            "connection": {
                "parameters": [
                    {"name": "token", "required": True},
                    {"name": "timeout", "required": False},
                ]
            },
            "external_options_allowlist": "opt1,opt2",
        }
        required, optional, allowlist = parse_connector_spec_legacy(spec)
        assert required == {"token"}
        assert optional == {"timeout"}
        assert allowlist == "opt1,opt2"

    def test_legacy_auth_methods(self):
        """Test legacy format for auth_methods - all auth params become optional."""
        spec = {
            "connection": {
                "auth_methods": [
                    {
                        "name": "method1",
                        "parameters": [{"name": "param1", "required": True}],
                    },
                ],
                "common_parameters": [
                    {"name": "common_req", "required": True},
                ],
            },
            "external_options_allowlist": "",
        }
        required, optional, allowlist = parse_connector_spec_legacy(spec)
        # Only common required params are required in legacy format
        assert required == {"common_req"}
        # Auth method params become optional
        assert "param1" in optional


class TestMergeExternalOptionsAllowlist:
    """Tests for merge_external_options_allowlist function."""

    def test_merge_both_non_empty(self):
        """Test merging two non-empty allowlists."""
        result = merge_external_options_allowlist("a,b,c", "c,d,e")
        # Should be sorted and deduplicated
        assert result == "a,b,c,d,e"

    def test_merge_source_empty(self):
        """Test merging when source is empty."""
        result = merge_external_options_allowlist("", "a,b")
        assert result == "a,b"

    def test_merge_constant_empty(self):
        """Test merging when constant is empty."""
        result = merge_external_options_allowlist("a,b", "")
        assert result == "a,b"

    def test_merge_both_empty(self):
        """Test merging when both are empty."""
        result = merge_external_options_allowlist("", "")
        assert result == ""

    def test_merge_strips_whitespace(self):
        """Test that whitespace is stripped."""
        result = merge_external_options_allowlist(" a , b ", " c , d ")
        assert result == "a,b,c,d"


class TestDetectAuthMethod:
    """Tests for detect_auth_method function."""

    def test_detect_no_auth_methods(self):
        """Test detection returns None for flat params spec."""
        spec = ParsedConnectorSpec(required_params={"token"})
        result = detect_auth_method({"token": "value"}, spec)
        assert result is None

    def test_detect_first_method(self):
        """Test detection of first auth method."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(name="method1", description="", required_params={"a", "b"}),
                AuthMethod(name="method2", description="", required_params={"c"}),
            ]
        )
        result = detect_auth_method({"a": "1", "b": "2"}, spec)
        assert result is not None
        assert result.name == "method1"

    def test_detect_second_method(self):
        """Test detection of second auth method."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(name="method1", description="", required_params={"a", "b"}),
                AuthMethod(name="method2", description="", required_params={"c"}),
            ]
        )
        result = detect_auth_method({"c": "value"}, spec)
        assert result is not None
        assert result.name == "method2"

    def test_detect_no_match(self):
        """Test detection returns None when no method matches."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(name="method1", description="", required_params={"a", "b"}),
            ]
        )
        result = detect_auth_method({"a": "1"}, spec)  # Missing 'b'
        assert result is None

    def test_detect_best_match(self):
        """Test that best match is selected when multiple match."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(name="method1", description="", required_params={"a"}),
                AuthMethod(
                    name="method2",
                    description="",
                    required_params={"a"},
                    optional_params={"b"},
                ),
            ]
        )
        # Both match, but method2 has more overlap
        result = detect_auth_method({"a": "1", "b": "2"}, spec)
        assert result is not None
        assert result.name == "method2"


class TestValidateConnectionOptions:
    """Tests for validate_connection_options function."""

    def test_validate_flat_params_valid(self):
        """Test validation passes with flat parameters."""
        spec = ParsedConnectorSpec(
            required_params={"token", "api_key"},
            optional_params={"timeout"},
        )
        options = {"token": "abc", "api_key": "xyz"}
        result = validate_connection_options("test", options, spec)
        assert result.is_valid()
        assert result.errors == []
        assert result.detected_auth_method is None

    def test_validate_flat_params_missing(self):
        """Test validation fails when required params missing."""
        spec = ParsedConnectorSpec(
            required_params={"token", "api_key"},
            optional_params=set(),
        )
        options = {"token": "abc"}
        result = validate_connection_options("test", options, spec)
        assert not result.is_valid()
        assert len(result.errors) == 1
        assert "api_key" in result.errors[0]

    def test_validate_flat_params_unknown_error(self):
        """Test unknown params generate error."""
        spec = ParsedConnectorSpec(
            required_params={"token"},
            optional_params=set(),
        )
        options = {"token": "abc", "unknown": "value"}
        result = validate_connection_options("test", options, spec)
        assert not result.is_valid()  # Has errors
        assert len(result.errors) == 1
        assert "unknown" in result.errors[0]
        assert "Known parameters are" in result.errors[0]

    def test_validate_auth_methods_valid(self):
        """Test validation passes with auth methods."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(name="api_key", description="", required_params={"api_key"}),
            ],
            common_required_params=set(),
        )
        options = {"api_key": "value"}
        result = validate_connection_options("test", options, spec)
        assert result.is_valid()
        assert result.detected_auth_method == "api_key"

    def test_validate_auth_methods_no_match(self):
        """Test validation fails when no auth method matches."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(name="method1", description="", required_params={"a", "b"}),
                AuthMethod(name="method2", description="", required_params={"c"}),
            ],
        )
        options = {"a": "1"}  # Missing 'b' for method1
        result = validate_connection_options("test", options, spec)
        assert not result.is_valid()
        assert "No valid authentication method detected" in result.errors[0]

    def test_validate_auth_methods_missing_common(self):
        """Test validation fails when common required params missing."""
        spec = ParsedConnectorSpec(
            auth_methods=[
                AuthMethod(name="api_key", description="", required_params={"api_key"}),
            ],
            common_required_params={"subdomain"},
        )
        options = {"api_key": "value"}  # Missing subdomain
        result = validate_connection_options("test", options, spec)
        assert not result.is_valid()
        assert "subdomain" in result.errors[0]

    def test_validate_allows_special_params(self):
        """Test that sourceName and externalOptionsAllowList are always allowed."""
        spec = ParsedConnectorSpec(required_params={"token"})
        options = {
            "token": "abc",
            "sourceName": "github",
            "externalOptionsAllowList": "opt1",
        }
        result = validate_connection_options("test", options, spec)
        assert result.is_valid()
        assert len(result.warnings) == 0


class TestValidateConnectionOptionsLegacy:
    """Tests for validate_connection_options_legacy function."""

    def test_validate_all_required_present(self):
        """Test validation passes when all required present."""
        result = validate_connection_options_legacy(
            "test",
            {"token": "abc", "key": "xyz"},
            {"token", "key"},
            {"timeout"},
        )
        assert result.is_valid()

    def test_validate_missing_required(self):
        """Test validation fails when required missing."""
        result = validate_connection_options_legacy(
            "test",
            {"token": "abc"},
            {"token", "key"},
            set(),
        )
        assert not result.is_valid()
        assert "key" in result.errors[0]

    def test_validate_unknown_error(self):
        """Test unknown params generate error."""
        result = validate_connection_options_legacy(
            "test",
            {"token": "abc", "unknown": "value"},
            {"token"},
            set(),
        )
        assert not result.is_valid()
        assert len(result.errors) == 1
        assert "unknown" in result.errors[0]


class TestValidationResult:
    """Tests for ValidationResult class."""

    def test_is_valid_no_errors(self):
        """Test is_valid returns True when no errors."""
        result = ValidationResult()
        assert result.is_valid()

    def test_is_valid_with_errors(self):
        """Test is_valid returns False when errors exist."""
        result = ValidationResult(errors=["Error 1"])
        assert not result.is_valid()

    def test_is_valid_with_warnings_only(self):
        """Test is_valid returns True with only warnings."""
        result = ValidationResult(warnings=["Warning 1"])
        assert result.is_valid()

