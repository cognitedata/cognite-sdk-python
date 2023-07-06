from typing import Any, Dict, Iterator

import pytest
from _pytest.mark import ParameterSet

from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling import filters as f
from cognite.client.data_classes.data_modeling import query as q


def result_set_expression_load_and_dump_equals_data() -> Iterator[ParameterSet]:
    raw = {
        "nodes": {
            "filter": {"equals": {"property": ["node", "externalId"], "value": {"parameter": "airplaneExternalId"}}}
        },
        "limit": 1,
    }
    loaded_node = q.NodeResultSetExpression(
        filter=f.Equals(property=["node", "externalId"], value={"parameter": "airplaneExternalId"}), limit=1
    )
    yield pytest.param(
        raw,
        loaded_node,
        id="Documentation Example",
    )

    raw = {
        "nodes": {
            "filter": {"range": {"lt": 2000, "property": ["IntegrationTestsImmutable", "Movie/2", "releaseYear"]}}
        }
    }
    loaded_node = q.NodeResultSetExpression(
        filter=f.Range(lt=2000, property=["IntegrationTestsImmutable", "Movie/2", "releaseYear"])
    )

    yield pytest.param(
        raw,
        loaded_node,
        id="Filter Node on Range",
    )

    raw = {
        "edges": {
            "direction": "outwards",
            "filter": {
                "equals": {"property": ["edge", "type"], "value": {"space": "MovieSpace", "externalId": "Movie.actors"}}
            },
        }
    }
    loaded_edge = q.EdgeResultSetExpression(
        filter=f.Equals(property=["edge", "type"], value={"space": "MovieSpace", "externalId": "Movie.actors"})
    )
    yield pytest.param(
        raw,
        loaded_edge,
        id="Filter Edge on Type",
    )


class TestResultSetExpressions:
    @pytest.mark.parametrize("raw_data, loaded", list(result_set_expression_load_and_dump_equals_data()))
    def test_load_and_dump_equals(self, raw_data: dict, loaded: q.ResultSetExpression) -> None:
        assert raw_data == q.ResultSetExpression.load(raw_data).dump(camel_case=True)

    @pytest.mark.parametrize("raw_data, loaded", list(result_set_expression_load_and_dump_equals_data()))
    def test_dump_load_equals(self, raw_data: dict, loaded: q.ResultSetExpression) -> None:
        assert loaded == q.ResultSetExpression.load(loaded.dump(camel_case=True))

    @pytest.mark.parametrize("raw_data, loaded", list(result_set_expression_load_and_dump_equals_data()))
    def test_load(self, raw_data: dict, loaded: q.ResultSetExpression) -> None:
        assert q.ResultSetExpression.load(raw_data) == loaded

    @pytest.mark.parametrize("raw_data, loaded", list(result_set_expression_load_and_dump_equals_data()))
    def test_dump(self, raw_data: dict, loaded: q.ResultSetExpression) -> None:
        assert loaded.dump(camel_case=True) == raw_data


def select_load_and_dump_equals_data() -> Iterator[ParameterSet]:
    raw: Dict[str, Any] = {}
    loaded = q.Select()
    yield pytest.param(raw, loaded, id="Empty")

    raw = {
        "sources": [
            {
                "properties": ["title"],
                "source": {"externalId": "Movie", "space": "IntegrationTestsImmutable", "type": "view", "version": "2"},
            }
        ]
    }
    loaded = q.Select(
        [q.SourceSelector(ViewId(space="IntegrationTestsImmutable", external_id="Movie", version="2"), ["title"])]
    )
    yield pytest.param(raw, loaded, id="Select single property")


class TestSelect:
    @pytest.mark.parametrize("raw_data, loaded", list(select_load_and_dump_equals_data()))
    def test_load_and_dump_equals(self, raw_data: dict, loaded: q.Select) -> None:
        assert raw_data == q.Select.load(raw_data).dump(camel_case=True)

    @pytest.mark.parametrize("raw_data, loaded", list(select_load_and_dump_equals_data()))
    def test_dump_load_equals(self, raw_data: dict, loaded: q.Select) -> None:
        assert loaded == q.Select.load(loaded.dump(camel_case=True))

    @pytest.mark.parametrize("raw_data, loaded", list(select_load_and_dump_equals_data()))
    def test_load(self, raw_data: dict, loaded: q.Select) -> None:
        assert q.Select.load(raw_data) == loaded

    @pytest.mark.parametrize("raw_data, loaded", list(select_load_and_dump_equals_data()))
    def test_dump(self, raw_data: dict, loaded: q.Select) -> None:
        assert loaded.dump(camel_case=True) == raw_data
