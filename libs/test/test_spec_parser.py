import pytest

from libs.spec_parser import SpecParser


def _build_valid_spec():
    return {
        "connection_name": "my_connection",
        "objects": [
            {
                "table": {
                    "source_table": "table_one",
                }
            },
            {
                "table": {
                    "source_table": "table_two",
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_2",
                        "primary_keys": ["id", "created_at"],
                        "sequence_by": "updated_at",
                        "some_flag": True,
                        "nested": {"key": "value"},
                    },
                }
            },
        ],
    }


def test_spec_parser_valid_spec_parses_and_exposes_fields():
    spec_dict = _build_valid_spec()

    parser = SpecParser(spec_dict)

    assert parser.connection_name() == "my_connection"
    assert parser.get_table_list() == ["table_one", "table_two"]

    # table without configuration returns empty dict
    assert parser.get_table_configuration("table_one") == {}

    # table with configuration returns the dict WITHOUT special keys
    config = parser.get_table_configuration("table_two")
    assert isinstance(config, dict)

    # special keys are excluded from table_configuration
    assert "scd_type" not in config
    assert "primary_keys" not in config
    assert "sequence_by" not in config

    # regular values are normalised to strings
    assert config["some_flag"] == "True"

    # nested structures are JSON-encoded
    import json

    nested_value = config["nested"]
    assert isinstance(nested_value, str)
    assert json.loads(nested_value) == {"key": "value"}

    # unknown table returns empty dict
    assert parser.get_table_configuration("unknown_table") == {}

    # Special keys are accessible via dedicated methods
    assert parser.get_scd_type("table_two") == "SCD_TYPE_2"
    assert parser.get_primary_keys("table_two") == ["id", "created_at"]
    assert parser.get_sequence_by("table_two") == "updated_at"

    # Special keys return None for tables without them
    assert parser.get_scd_type("table_one") is None
    assert parser.get_primary_keys("table_one") is None
    assert parser.get_sequence_by("table_one") is None

    # Special keys return None for unknown tables
    assert parser.get_scd_type("unknown_table") is None
    assert parser.get_primary_keys("unknown_table") is None
    assert parser.get_sequence_by("unknown_table") is None


@pytest.mark.parametrize(
    "spec",
    [
        [],  # not a dict
        # missing connection_name
        {
            "objects": [
                {"table": {"source_table": "t1"}},
            ]
        },
        # empty connection_name
        {
            "connection_name": "   ",
            "objects": [
                {"table": {"source_table": "t1"}},
            ],
        },
        # missing objects
        {
            "connection_name": "conn",
        },
        # objects is not a list
        {
            "connection_name": "conn",
            "objects": "not-a-list",
        },
        # objects is an empty list
        {
            "connection_name": "conn",
            "objects": [],
        },
        # object without table key
        {
            "connection_name": "conn",
            "objects": [{}],
        },
        # table without source_table
        {
            "connection_name": "conn",
            "objects": [
                {"table": {}},
            ],
        },
        # extra top-level field not allowed
        {
            "connection_name": "conn",
            "objects": [
                {"table": {"source_table": "t1"}},
            ],
            "extra": "not-allowed",
        },
        # extra field in table not allowed
        {
            "connection_name": "conn",
            "objects": [
                {
                    "table": {
                        "source_table": "t1",
                        "unexpected": "field",
                    }
                },
            ],
        },
    ],
)
def test_spec_parser_invalid_specs_raise_value_error(spec):
    if not isinstance(spec, dict):
        # pre-validation type check
        with pytest.raises(ValueError, match="Spec must be a dictionary"):
            SpecParser(spec)  # type: ignore[arg-type]
    else:
        # pydantic validation errors are wrapped in ValueError
        with pytest.raises(ValueError, match="Invalid pipeline spec"):
            SpecParser(spec)


def test_scd_type_validation_case_insensitive():
    """Test that SCD type is case-insensitive and normalized to uppercase."""
    spec = {
        "connection_name": "test_conn",
        "objects": [
            {
                "table": {
                    "source_table": "test_table",
                    "table_configuration": {
                        "scd_type": "scd_type_2",  # lowercase
                    },
                }
            },
        ],
    }
    parser = SpecParser(spec)
    # Should be normalized to uppercase
    assert parser.get_scd_type("test_table") == "SCD_TYPE_2"


@pytest.mark.parametrize(
    "scd_type_value,expected",
    [
        ("SCD_TYPE_1", "SCD_TYPE_1"),
        ("scd_type_1", "SCD_TYPE_1"),
        ("Scd_Type_1", "SCD_TYPE_1"),
        ("SCD_TYPE_2", "SCD_TYPE_2"),
        ("scd_type_2", "SCD_TYPE_2"),
        ("APPEND_ONLY", "APPEND_ONLY"),
        ("append_only", "APPEND_ONLY"),
        ("Append_Only", "APPEND_ONLY"),
    ],
)
def test_scd_type_all_valid_values_case_insensitive(scd_type_value, expected):
    """Test all valid SCD types in various cases are normalized correctly."""
    spec = {
        "connection_name": "test_conn",
        "objects": [
            {
                "table": {
                    "source_table": "test_table",
                    "table_configuration": {
                        "scd_type": scd_type_value,
                    },
                }
            },
        ],
    }
    parser = SpecParser(spec)
    assert parser.get_scd_type("test_table") == expected


@pytest.mark.parametrize(
    "invalid_scd_type",
    [
        "SCD_TYPE_3",
        "INVALID",
        "snapshot",
        "incremental",
        "",
        "scd_type",
    ],
)
def test_scd_type_invalid_value_raises_error(invalid_scd_type):
    """Test that invalid SCD types raise ValueError."""
    spec = {
        "connection_name": "test_conn",
        "objects": [
            {
                "table": {
                    "source_table": "test_table",
                    "table_configuration": {
                        "scd_type": invalid_scd_type,
                    },
                }
            },
        ],
    }
    parser = SpecParser(spec)

    with pytest.raises(ValueError, match="Invalid SCD type"):
        parser.get_scd_type("test_table")


def test_get_full_destination_table_name_with_all_fields():
    """Test full destination name when all destination fields are specified."""
    spec = {
        "connection_name": "test_conn",
        "objects": [
            {
                "table": {
                    "source_table": "source_table",
                    "destination_catalog": "my_catalog",
                    "destination_schema": "my_schema",
                    "destination_table": "my_dest_table",
                }
            },
        ],
    }
    parser = SpecParser(spec)
    assert (
        parser.get_full_destination_table_name("source_table")
        == "`my_catalog`.`my_schema`.`my_dest_table`"
    )


def test_get_full_destination_table_name_without_destination_table():
    """Test that source table name is used when destination_table is not specified."""
    spec = {
        "connection_name": "test_conn",
        "objects": [
            {
                "table": {
                    "source_table": "source_table",
                    "destination_catalog": "my_catalog",
                    "destination_schema": "my_schema",
                }
            },
        ],
    }
    parser = SpecParser(spec)
    assert (
        parser.get_full_destination_table_name("source_table")
        == "`my_catalog`.`my_schema`.`source_table`"
    )


def test_get_full_destination_table_name_without_catalog():
    """Test that only table name is returned when catalog is missing."""
    spec = {
        "connection_name": "test_conn",
        "objects": [
            {
                "table": {
                    "source_table": "source_table",
                    "destination_schema": "my_schema",
                    "destination_table": "my_dest_table",
                }
            },
        ],
    }
    parser = SpecParser(spec)
    assert parser.get_full_destination_table_name("source_table") == "my_dest_table"


def test_get_full_destination_table_name_without_schema():
    """Test that only table name is returned when schema is missing."""
    spec = {
        "connection_name": "test_conn",
        "objects": [
            {
                "table": {
                    "source_table": "source_table",
                    "destination_catalog": "my_catalog",
                    "destination_table": "my_dest_table",
                }
            },
        ],
    }
    parser = SpecParser(spec)
    assert parser.get_full_destination_table_name("source_table") == "my_dest_table"


def test_get_full_destination_table_name_no_destination_fields():
    """Test that source table name is returned when no destination fields are specified."""
    spec = {
        "connection_name": "test_conn",
        "objects": [
            {
                "table": {
                    "source_table": "source_table",
                }
            },
        ],
    }
    parser = SpecParser(spec)
    assert parser.get_full_destination_table_name("source_table") == "source_table"


def test_get_full_destination_table_name_unknown_table_raises_error():
    """Test that ValueError is raised for unknown table."""
    spec = {
        "connection_name": "test_conn",
        "objects": [
            {
                "table": {
                    "source_table": "source_table",
                }
            },
        ],
    }
    parser = SpecParser(spec)

    with pytest.raises(ValueError, match="Table 'unknown_table' not found"):
        parser.get_full_destination_table_name("unknown_table")
