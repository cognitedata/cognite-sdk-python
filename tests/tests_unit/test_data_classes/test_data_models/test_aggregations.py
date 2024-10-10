from collections.abc import Iterator

import pytest
from _pytest.mark import ParameterSet

from cognite.client.data_classes.aggregations import AggregatedValue, Aggregation


def aggregation_load_and_dump_equals_data() -> Iterator[ParameterSet]:
    yield pytest.param({"histogram": {"property": "weight", "interval": 3.0}}, id="histogram")
    yield pytest.param({"avg": {"property": "weight"}}, id="avg")
    yield pytest.param({"min": {"property": "height"}}, id="min")
    yield pytest.param({"max": {"property": "height"}}, id="max")
    yield pytest.param({"sum": {"property": "price"}}, id="sum")
    yield pytest.param({"count": {"property": "instances"}}, id="count")


@pytest.mark.parametrize("raw_data", list(aggregation_load_and_dump_equals_data()))
def test_aggregation_load_and_dump_equals(raw_data: dict) -> None:
    parsed = Aggregation.load(raw_data)

    dumped = parsed.dump()

    assert dumped == raw_data


def aggregated_value_load_and_dump_equals_data() -> Iterator[ParameterSet]:
    yield pytest.param(
        {
            "aggregate": "histogram",
            "interval": 3.0,
            "property": "weight",
            "buckets": [{"count": 1, "start": 1.0}, {"start": 4.0, "count": 5}],
        },
        id="histogram",
    )
    yield pytest.param({"aggregate": "count", "property": "instances", "value": 5}, id="count")
    yield pytest.param({"aggregate": "max", "property": "instances", "value": 5}, id="max")
    yield pytest.param({"aggregate": "min", "property": "instances", "value": 5}, id="min")
    yield pytest.param({"aggregate": "sum", "property": "instances", "value": 5}, id="sum")
    yield pytest.param({"aggregate": "avg", "property": "instances", "value": 5}, id="avg")


@pytest.mark.parametrize("raw_data", list(aggregated_value_load_and_dump_equals_data()))
def test_aggregated_value_load_and_dump_equals(raw_data: dict) -> None:
    parsed = AggregatedValue.load(raw_data)

    dumped = parsed.dump()

    assert dumped == raw_data
