import pytest

from cognite.client.data_classes.data_modeling import dsl_filter as dsl
from cognite.client.data_classes.data_modeling.dsl_filter import dump_dsl_filter, load_dsl_filter


def load_and_dump_equals_data():
    yield pytest.param(
        {
            "and": [
                {"in": {"property": ["tag"], "values": ["1001_1", "1001_1"]}},
                {"range": {"property": ["weight"], "gte": 0}},
            ]
        },
        id="In and Range",
    )

    yield pytest.param(
        {
            "or": [
                {
                    "equals": {"property": ["name"], "value": "Quentin Tarantino"},
                    "prefix": {"property": ["full_name"], "value": "Dr."},
                    "exists": {
                        "property": ["room"],
                    },
                }
            ]
        },
        id="Equals or Prefix or Exists",
    )


@pytest.mark.parametrize("raw_data", list(load_and_dump_equals_data()))
def test_load_and_dump_equals(raw_data: dict):
    parsed = load_dsl_filter(raw_data)

    dumped = dump_dsl_filter(parsed)

    assert dumped == raw_data


def test_dump_filter():
    filter_ = {
        "or": [
            {"equals": dsl.EqualObject(["person", "name"], ["Quentin", "Tarantino"])},
            {"containsAny": dsl.ContainsAnyObject(["person", "name"], [["Quentin", "Tarantino"]])},
        ]
    }
    expected = {
        "or": [
            {"equals": {"property": ["person", "name"], "value": ["Quentin", "Tarantino"]}},
            {"containsAny": {"property": ["person", "name"], "values": [["Quentin", "Tarantino"]]}},
        ]
    }

    actual = dump_dsl_filter(filter_)

    assert actual == expected
