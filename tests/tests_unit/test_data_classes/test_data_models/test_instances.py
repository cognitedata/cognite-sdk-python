from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes.data_modeling import (
    ContainerId,
    DirectRelationReference,
    Edge,
    EdgeApply,
    EdgeList,
    Node,
    NodeApply,
    NodeId,
    NodeList,
    NodeOrEdgeData,
)


class TestEdgeApply:
    def test_dump_and_load(self) -> None:
        edge = EdgeApply(
            space="mySpace",
            external_id="relation:arnold_schwarzenegger:actor",
            type=DirectRelationReference("mySpace", "Person.role"),
            start_node=DirectRelationReference("mySpace", "person.external_id"),
            end_node=DirectRelationReference("mySpace", "actor.external_id"),
        )

        assert EdgeApply.load(edge.dump(camel_case=True)).dump(camel_case=True) == {
            "space": "mySpace",
            "externalId": "relation:arnold_schwarzenegger:actor",
            "type": {
                "space": "mySpace",
                "externalId": "Person.role",
            },
            "instanceType": "edge",
            "sources": [],
            "startNode": {
                "space": "mySpace",
                "externalId": "person.external_id",
            },
            "endNode": {"space": "mySpace", "externalId": "actor.external_id"},
        }


class TestNodeOrEdgeData:
    def test_direct_relation_serialization(self) -> None:
        data = NodeOrEdgeData(
            source=ContainerId("IntegrationTestsImmutable", "Case"),
            properties=dict(
                name="Integration test",
                some_direct_relation=DirectRelationReference("space", "external_id"),
                another_direct_relation_type=NodeId("space", "external_id"),
            ),
        )
        assert {
            "properties": {
                "another_direct_relation_type": {"external_id": "external_id", "space": "space"},
                "name": "Integration test",
                "some_direct_relation": {"external_id": "external_id", "space": "space"},
            },
            "source": {"external_id": "Case", "space": "IntegrationTestsImmutable", "type": "container"},
        } == data.dump(camel_case=False)


class TestNodeApply:
    def test_dump_and_load(self) -> None:
        node = NodeApply(
            space="IntegrationTestsImmutable",
            external_id="shop:case:integration_test",
            type=("someSpace", "someType"),
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

        assert NodeApply.load(node.dump(camel_case=True)).dump(camel_case=True) == {
            "externalId": "shop:case:integration_test",
            "instanceType": "node",
            "sources": [
                {
                    "properties": {
                        "arguments": "Integration test",
                        "bid": "shop:bid_matrix:8",
                        "bid_history": ["shop:bid_matrix:9"],
                        "commands": {
                            "externalId": "shop:command_config:integration_test",
                            "space": "IntegrationTestsImmutable",
                        },
                        "cut_files": ["shop:cut_file:1"],
                        "end_time": "2021-01-01T00:00:00",
                        "name": "Integration test",
                        "runStatus": "Running",
                        "scenario": "Integration test",
                        "start_time": "2021-01-01T00:00:00",
                    },
                    "source": {"externalId": "Case", "space": "IntegrationTestsImmutable", "type": "container"},
                }
            ],
            "space": "IntegrationTestsImmutable",
            "type": {"externalId": "someType", "space": "someSpace"},
        }


class TestNode:
    def test_dump_and_load(self) -> None:
        node = Node(
            space="IntegrationTestsImmutable",
            external_id="shop:case:integration_test",
            version=1,
            type=DirectRelationReference("someSpace", "someType"),
            last_updated_time=123,
            created_time=123,
            deleted_time=None,
            properties=None,
        )

        assert Node.load(node.dump(camel_case=True)).dump(camel_case=True) == {
            "createdTime": 123,
            "externalId": "shop:case:integration_test",
            "instanceType": "node",
            "lastUpdatedTime": 123,
            "properties": {},
            "space": "IntegrationTestsImmutable",
            "type": {"externalId": "someType", "space": "someSpace"},
            "version": 1,
        }


@pytest.fixture
def node_dumped() -> dict[str, Any]:
    return {
        "space": "craft",
        "externalId": "xid",
        "version": "V",
        "lastUpdatedTime": 123,
        "createdTime": 123,
        "properties": {
            "my-space": {"my-view/v8": {"num": "210113347", "jsån": {"why": "is", "this": "here"}, "title": "sir"}}
        },
    }


@pytest.fixture
def edge_dumped(node_dumped: dict[str, Any]) -> dict[str, Any]:
    return {
        **node_dumped,
        "type": {"space": "sp", "externalId": "xid"},
        "startNode": {"space": "spsp", "externalId": "xid2"},
        "endNode": {"space": "spspsp", "externalId": "xid3"},
    }


@pytest.mark.dsl
class TestInstancesToPandas:
    @pytest.mark.parametrize("inst_cls", (Node, Edge))
    def test_expand_properties(
        self, node_dumped: dict[str, Any], edge_dumped: dict[str, Any], inst_cls: type[Node] | type[Edge]
    ) -> None:
        raw = node_dumped if inst_cls is Node else edge_dumped
        not_expanded = inst_cls._load(raw).to_pandas(expand_properties=False)
        expanded = inst_cls._load(raw).to_pandas(expand_properties=True, remove_property_prefix=True)
        expanded_with_prefix = inst_cls._load(raw).to_pandas(expand_properties=True, remove_property_prefix=False)

        assert "properties" in not_expanded.index
        assert "properties" not in expanded.index
        assert "properties" not in expanded_with_prefix.index

        assert raw["properties"] == not_expanded.loc["properties"].item()

        for k, v in raw["properties"]["my-space"]["my-view/v8"].items():
            assert v == expanded.loc[k].item()
            assert v == expanded_with_prefix.loc[f"my-space.my-view/v8.{k}"].item()

    @pytest.mark.parametrize("inst_cls", (NodeList, EdgeList))
    def test_expand_properties__list_class(
        self, node_dumped: dict[str, Any], edge_dumped: dict[str, Any], inst_cls: type[NodeList] | type[EdgeList]
    ) -> None:
        raw = node_dumped if inst_cls is Node else edge_dumped
        not_expanded = inst_cls._load([raw, raw]).to_pandas(expand_properties=False)
        expanded = inst_cls._load([raw, raw]).to_pandas(expand_properties=True, remove_property_prefix=True)
        expanded_with_prefix = inst_cls._load([raw, raw]).to_pandas(
            expand_properties=True, remove_property_prefix=False
        )

        assert "properties" in not_expanded.columns
        assert "properties" not in expanded.columns
        assert "properties" not in expanded_with_prefix.columns

        assert raw["properties"] == not_expanded.loc[0, "properties"]

        for k, v in raw["properties"]["my-space"]["my-view/v8"].items():
            assert v == expanded.loc[0, k]
            assert v == expanded_with_prefix.loc[0, f"my-space.my-view/v8.{k}"]
