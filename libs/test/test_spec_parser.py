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

    # table without configuration returns None
    assert parser.get_table_configuration("table_one") is None

    # table with configuration returns the full dict
    config = parser.get_table_configuration("table_two")
    assert isinstance(config, dict)

    # values are normalised to strings
    assert config["scd_type"] == "SCD_TYPE_2"
    assert config["some_flag"] == "True"

    # nested structures are JSON-encoded
    import json

    nested_value = config["nested"]
    assert isinstance(nested_value, str)
    assert json.loads(nested_value) == {"key": "value"}

    # unknown table returns None
    assert parser.get_table_configuration("unknown_table") is None


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
