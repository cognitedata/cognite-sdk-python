from typing import Any, Dict, Iterator

import pytest
from _pytest.mark import ParameterSet

from cognite.client.data_classes.data_modeling import (
    ContainerId,
    DirectRelationReference,
    EdgeApply,
    NodeApply,
    NodeOrEdgeData,
    ViewId,
)
from cognite.client.data_classes.data_modeling.instances import Select, SourceSelector


class TestEdgeApply:
    def test_dump(self) -> None:
        edge = EdgeApply(
            space="mySpace",
            external_id="relation:arnold_schwarzenegger:actor",
            type=DirectRelationReference("mySpace", "Person.role"),
            start_node=DirectRelationReference("mySpace", "person.external_id"),
            end_node=DirectRelationReference("mySpace", "actor.external_id"),
        )

        assert edge.dump(camel_case=True) == {
            "space": "mySpace",
            "externalId": "relation:arnold_schwarzenegger:actor",
            "type": {
                "space": "mySpace",
                "externalId": "Person.role",
            },
            "instanceType": "edge",
            "startNode": {
                "space": "mySpace",
                "externalId": "person.external_id",
            },
            "endNode": {"space": "mySpace", "externalId": "actor.external_id"},
        }


class TestNodeApply:
    def test_dump_with_snake_case_fields(self) -> None:
        # Arrange
        node = NodeApply(
            space="IntegrationTestsImmutable",
            external_id="shop:case:integration_test",
            sources=[
                NodeOrEdgeData(
                    source=ContainerId("IntegrationTestsImmutable", "Case"),
                    properties=dict(
                        name="Integration test",
                        scenario="Integration test",
                        start_time="2021-01-01T00:00:00",
                        end_time="2021-01-01T00:00:00",
                        cut_files=["shop:cut_file:1"],
                        bid="shop:bid_matrix:8",
                        bid_history=["shop:bid_matrix:9"],
                        runStatus="Running",
                        arguments="Integration test",
                        commands={
                            "space": "IntegrationTestsImmutable",
                            "externalId": "shop:command_config:integration_test",
                        },
                    ),
                )
            ],
        )

        # Act
        dumped = node.dump(camel_case=True)

        # Assert
        assert sorted(dumped["sources"][0]["properties"]) == sorted(
            [
                "name",
                "scenario",
                "start_time",
                "end_time",
                "cut_files",
                "bid",
                "bid_history",
                "runStatus",
                "arguments",
                "commands",
            ]
        )


def select_load_and_dump_equals_data() -> Iterator[ParameterSet]:
    raw: Dict[str, Any] = {}
    loaded = Select()
    yield pytest.param(raw, loaded, id="Empty")

    raw = {
        "sources": [
            {
                "properties": ["title"],
                "source": {"externalId": "Movie", "space": "IntegrationTestsImmutable", "type": "view", "version": "2"},
            }
        ]
    }
    loaded = Select(
        [SourceSelector(ViewId(space="IntegrationTestsImmutable", external_id="Movie", version="2"), ["title"])]
    )
    yield pytest.param(raw, loaded, id="Select single property")


class TestSelect:
    @pytest.mark.parametrize("raw_data, loaded", list(select_load_and_dump_equals_data()))
    def test_load_and_dump_equals(self, raw_data: dict, loaded: Select) -> None:
        assert raw_data == Select.load(raw_data).dump(camel_case=True)

    @pytest.mark.parametrize("raw_data, loaded", list(select_load_and_dump_equals_data()))
    def test_dump_load_equals(self, raw_data: dict, loaded: Select) -> None:
        assert loaded == Select.load(loaded.dump(camel_case=True))

    @pytest.mark.parametrize("raw_data, loaded", list(select_load_and_dump_equals_data()))
    def test_load(self, raw_data: dict, loaded: Select) -> None:
        assert Select.load(raw_data) == loaded

    @pytest.mark.parametrize("raw_data, loaded", list(select_load_and_dump_equals_data()))
    def test_dump(self, raw_data: dict, loaded: Select) -> None:
        assert loaded.dump(camel_case=True) == raw_data
