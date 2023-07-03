from typing import Iterator

import pytest
from _pytest.mark import ParameterSet

from cognite.client.data_classes.data_modeling import filters as f
from cognite.client.data_classes.data_modeling import queries as q


def load_and_dump_equals_data() -> Iterator[ParameterSet]:
    raw = {
        "nodes": {
            "filter": {"equals": {"property": ["node", "externalId"], "value": {"parameter": "airplaneExternalId"}}}
        },
        "limit": 1,
    }
    loaded = q.QueryNodeTableExpression(
        q.QueryNode(filter=f.Equals(property=["node", "externalId"], value={"parameter": "airplaneExternalId"})),
        limit=1,
    )
    yield pytest.param(
        raw,
        loaded,
        id="Documetation Example",
    )

    raw = {
        "nodes": {
            "filter": {"range": {"lt": 2000, "property": ["IntegrationTestsImmutable", "Movie/2", "releaseYear"]}}
        }
    }
    loaded = q.QueryNodeTableExpression(
        q.QueryNode(filter=f.Range(lt=2000, property=["IntegrationTestsImmutable", "Movie/2", "releaseYear"]))
    )

    yield pytest.param(
        raw,
        loaded,
        id="Simple node filter",
    )


@pytest.mark.parametrize("raw_data, loaded", list(load_and_dump_equals_data()))
def test_load_and_dump_equals(raw_data: dict, loaded: q.Query) -> None:
    assert raw_data == q.Query.load(raw_data).dump(camel_case=True)


@pytest.mark.parametrize("raw_data, loaded", list(load_and_dump_equals_data()))
def test_dump_load_equals(raw_data: dict, loaded: q.Query) -> None:
    assert loaded == q.Query.load(loaded.dump(camel_case=True))


@pytest.mark.parametrize("raw_data, loaded", list(load_and_dump_equals_data()))
def test_load(raw_data: dict, loaded: q.Query) -> None:
    assert q.Query.load(raw_data) == loaded


@pytest.mark.parametrize("raw_data, loaded", list(load_and_dump_equals_data()))
def test_dump(raw_data: dict, loaded: q.Query) -> None:
    assert loaded.dump(camel_case=True) == raw_data
