import textwrap
from collections.abc import Iterator
from typing import Any

import pytest
from _pytest.mark import ParameterSet

from cognite.client.data_classes import filters as f
from cognite.client.data_classes.data_modeling import InstanceSort, ViewId
from cognite.client.data_classes.data_modeling import query as q
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFile


def result_set_expression_load_and_dump_equals_data() -> Iterator[ParameterSet]:
    raw = {
        "nodes": {
            "filter": {"equals": {"property": ["node", "externalId"], "value": {"parameter": "airplaneExternalId"}}},
            "from": "bla",
            "through": {
                "source": {"type": "view", "space": "some", "externalId": "extid", "version": "v1"},
                "identifier": "bla",
            },
            "chainTo": "destination",
            "direction": "outwards",
        },
        "limit": 1,
    }
    loaded_node = q.NodeResultSetExpression(
        filter=f.Equals(property=["node", "externalId"], value={"parameter": "airplaneExternalId"}),
        limit=1,
        from_="bla",
        through=["some", "extid/v1", "bla"],
    )
    yield pytest.param(raw, loaded_node, id="NTE with through view reference")

    raw = {
        "nodes": {
            "filter": {"equals": {"property": ["node", "externalId"], "value": {"parameter": "airplaneExternalId"}}},
            "from": "bla",
            "through": {
                "source": {"type": "container", "space": "some", "externalId": "extid"},
                "identifier": "bla",
            },
            "chainTo": "destination",
            "direction": "outwards",
        },
        "limit": 1,
    }
    loaded_node = q.NodeResultSetExpression(
        filter=f.Equals(property=["node", "externalId"], value={"parameter": "airplaneExternalId"}),
        limit=1,
        from_="bla",
        through=["some", "extid", "bla"],
    )
    yield pytest.param(raw, loaded_node, id="NTE with through container reference")

    raw = {
        "nodes": {
            "filter": {"range": {"lt": 2000, "property": ["IntegrationTestsImmutable", "Movie/2", "releaseYear"]}},
            "chainTo": "destination",
            "direction": "outwards",
        }
    }
    loaded_node = q.NodeResultSetExpression(
        filter=f.Range(lt=2000, property=["IntegrationTestsImmutable", "Movie/2", "releaseYear"])
    )

    yield pytest.param(raw, loaded_node, id="Filter Node on Range")

    raw = {
        "edges": {
            "direction": "outwards",
            "filter": {
                "equals": {"property": ["edge", "type"], "value": {"space": "MovieSpace", "externalId": "Movie.actors"}}
            },
            "chainTo": "destination",
        }
    }
    loaded_edge = q.EdgeResultSetExpression(
        filter=f.Equals(property=["edge", "type"], value={"space": "MovieSpace", "externalId": "Movie.actors"})
    )
    yield pytest.param(raw, loaded_edge, id="Filter Edge on Type")

    raw = {
        "union": [
            "1",
            {
                "intersection": [
                    "2",
                    {
                        "unionAll": ["3", "4"],
                        "except": ["5"],
                        "limit": 100,
                    },
                ],
                "except": ["5"],
                "limit": 100,
            },
        ],
        "except": ["5"],
        "limit": 100,
    }
    loaded_set_expression = q.Union(
        union=[
            "1",
            q.Intersection(
                intersection=[
                    "2",
                    q.UnionAll(
                        union_all=["3", "4"],
                        except_=["5"],
                        limit=100,
                    ),
                ],
                except_=["5"],
                limit=100,
            ),
        ],
        except_=["5"],
        limit=100,
    )
    yield pytest.param(raw, loaded_set_expression, id="Deeply nested set operations")


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
    raw: dict[str, Any] = {}
    loaded = q.Select()
    yield pytest.param(raw, loaded, id="Empty")

    raw = {
        "sources": [
            {
                "properties": ["title"],
                "source": {"externalId": "Movie", "space": "IntegrationTestsImmutable", "type": "view", "version": "2"},
            }
        ],
        "sort": [
            {
                "property": ("IntegrationTestSpace", "Person", "name"),
                "direction": "descending",
                "nullsFirst": True,
            }
        ],
    }
    loaded = q.Select(
        [q.SourceSelector(ViewId(space="IntegrationTestsImmutable", external_id="Movie", version="2"), ["title"])],
        [InstanceSort(("IntegrationTestSpace", "Person", "name"), direction="descending", nulls_first=True)],
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


def query_load_yaml_data() -> Iterator[ParameterSet]:
    raw_yaml = """\
        with:
            airplanes:
                nodes:
                    filter:
                        equals:
                            property: ["node", "externalId"]
                            value: {"parameter": "airplaneExternalId"}
                    chainTo: destination
                    direction: outwards
                limit: 1
            lands_in_airports:
                edges:
                    from: airplanes
                    maxDistance: 1
                    direction: outwards
                    filter:
                        equals:
                            property: ["edge", "type"]
                            value: ["aviation", "lands-in"]
                    chainTo: destination
            airports:
                nodes:
                    chainTo: destination
                    direction: outwards
                    from: lands_in_airports
        parameters:
            airplaneExternalId: myFavouriteAirplane
        select:
            airplanes: {}
            airports: {}
    """
    expected = q.Query(
        with_={
            "airplanes": q.NodeResultSetExpression(
                filter=f.Equals(property=["node", "externalId"], value={"parameter": "airplaneExternalId"}), limit=1
            ),
            "lands_in_airports": q.EdgeResultSetExpression(
                from_="airplanes",
                max_distance=1,
                direction="outwards",
                filter=f.Equals(property=["edge", "type"], value=["aviation", "lands-in"]),
            ),
            "airports": q.NodeResultSetExpression(from_="lands_in_airports"),
        },
        parameters={"airplaneExternalId": "myFavouriteAirplane"},
        select={"airplanes": q.Select(), "airports": q.Select()},
    )
    yield pytest.param(textwrap.dedent(raw_yaml), expected, id="Documentation Example")

    raw_yaml = """\
      with:
        movies:
          nodes:
            filter:
              equals:
                property:
                - IntegrationTestsImmutable
                - Movie/2
                - releaseYear
                value: 1994
            chainTo: destination
            direction: outwards
      select:
        movies:
          sources:
          - source:
              space: IntegrationTestsImmutable
              externalId: Movie
              version: '2'
              type: view
            properties:
            - title
            - releaseYear
      cursors:
        movies: Z0FBQUFBQmtwc0RxQmducHpsWFd6VnZFdWwyWnFJbmxWS1BlT
    """
    movie_id = ViewId(space="IntegrationTestsImmutable", external_id="Movie", version="2")
    movies_released_1994 = q.NodeResultSetExpression(
        filter=f.Equals(list(movie_id.as_property_ref("releaseYear")), 1994)
    )
    expected = q.Query(
        with_={"movies": movies_released_1994},
        select={"movies": q.Select([q.SourceSelector(movie_id, ["title", "releaseYear"])])},
        cursors={"movies": "Z0FBQUFBQmtwc0RxQmducHpsWFd6VnZFdWwyWnFJbmxWS1BlT"},
    )
    yield pytest.param(textwrap.dedent(raw_yaml), expected, id="Example with cursors")


@pytest.fixture
def query_with_non_yaml_native_data_classes() -> q.Query:
    # YAML doesn't support tuple, so we want to test that yaml tags e.g. !!python/tuple does not end
    # up in the output when dumping a Query object.
    return q.Query(
        with_={
            "files": q.NodeResultSetExpression(
                filter=f.Exists(CogniteFile.get_source().as_property_ref("category")),
                limit=None,
            ),
            "categories": q.NodeResultSetExpression(
                from_="files",
                through=CogniteFile.get_source().as_property_ref("category"),
            ),
        },
        select={"categories": q.Select()},
    )


class TestQuery:
    @pytest.mark.parametrize("raw_data, expected", list(query_load_yaml_data()))
    def test_load_yaml(self, raw_data: str, expected: q.Query) -> None:
        actual = q.Query.load_yaml(raw_data)
        assert actual.dump(camel_case=True) == expected.dump(camel_case=True)

    def test_dump_yaml_no_tags(self, query_with_non_yaml_native_data_classes: q.Query) -> None:
        query = query_with_non_yaml_native_data_classes
        dumped = query.dump_yaml()
        assert "!!python/tuple" not in dumped

        # Load will now load tuple as list:
        loaded = q.Query.load_yaml(dumped)
        assert loaded != query

        # ...re-dump-loading should be equal to the first loaded
        dumped_again = loaded.dump_yaml()
        assert "!!python/tuple" not in dumped_again
        assert q.Query.load_yaml(dumped_again) == loaded
