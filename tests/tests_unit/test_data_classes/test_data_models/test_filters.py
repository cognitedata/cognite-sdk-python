import pytest

import cognite.client.data_classes.data_modeling.filters as f
from cognite.client.data_classes.data_modeling.filters import Filter


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
                {"equals": {"property": ["name"], "value": "Quentin Tarantino"}},
                {"prefix": {"property": ["full_name"], "value": "Dr."}},
                {
                    "exists": {
                        "property": ["room"],
                    }
                },
            ]
        },
        id="Equals or Prefix or Exists",
    )


@pytest.mark.parametrize("raw_data", list(load_and_dump_equals_data()))
def test_load_and_dump_equals(raw_data: dict):
    parsed = Filter.load(raw_data)

    dumped = parsed.dump()

    assert dumped == raw_data


def dump_filter_test_data():
    user_filter = f.Or(
        f.Equals(property=["person", "name"], value=["Quentin", "Tarantino"]),
        f.ContainsAny(property=["person", "name"], values=[["Quentin", "Tarantino"]]),
    )
    expected = {
        "or": [
            {"equals": {"property": ["person", "name"], "value": ["Quentin", "Tarantino"]}},
            {"containsAny": {"property": ["person", "name"], "values": [["Quentin", "Tarantino"]]}},
        ]
    }

    yield pytest.param(user_filter, expected, id="Or with Equals and ContainsAny")

    complex_filter = f.And(
        f.Nested(scope=("space", "container", "prop"), filter=f.MatchAll()),
        f.Or(
            f.HasData(containers=[("space", "container")]),
            f.Overlaps(
                start_property=("space", "container", "prop1"),
                end_property=("space", "container", "prop2"),
                lt=f.ParameterValue("lt"),
            ),
        ),
    )
    expected = {
        "and": [
            {
                "nested": {
                    "scope": ("space", "container", "prop"),
                    "filter": {"matchAll": {}},
                }
            },
            {
                "or": [
                    {"hasData": {"views": [], "containers": [("space", "container")]}},
                    {
                        "overlaps": {
                            "startProperty": ("space", "container", "prop1"),
                            "endProperty": ("space", "container", "prop2"),
                            "lt": {"parameter": "lt"},
                        }
                    },
                ]
            },
        ]
    }
    yield pytest.param(complex_filter, expected, id="And nested and Or with has data and overlaps")


@pytest.mark.parametrize("user_filter, expected", list(dump_filter_test_data()))
def test_dump_filter(user_filter: Filter, expected: dict):
    actual = user_filter.dump()

    assert actual == expected
