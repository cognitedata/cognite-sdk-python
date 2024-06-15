from __future__ import annotations

from dataclasses import dataclass
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
    ViewId,
)
from cognite.client.data_classes.data_modeling.instances import Instance, Properties, PropertyLike


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


@dataclass
class WindTurbine(PropertyLike):
    _source = ViewId("power-models", "WindTurbine", "v1")
    name: str
    wind_farm: str
    rotor: DirectRelationReference


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

    def test_custom_properties(self) -> None:
        node = NodeApply(
            space="IntegrationTestsImmutable",
            external_id="shop:case:integration_test",
            type=("someSpace", "someType"),
            sources=[
                NodeOrEdgeData(
                    source=WindTurbine._source,
                    properties=dict(
                        name="MyWindTurbine",
                        wind_farm="Utsira Nord",
                        rotor=DirectRelationReference("space", "external_id"),
                    ),
                )
            ],
        ).property_as_type(WindTurbine)

        assert isinstance(node.sources, WindTurbine)
        assert node.sources.name == "MyWindTurbine"
        assert node.sources.wind_farm == "Utsira Nord"
        assert node.sources.rotor == DirectRelationReference("space", "external_id")

        reloaded = NodeApply.load(node.dump()).sources
        assert isinstance(reloaded, list)
        assert isinstance(reloaded[0], NodeOrEdgeData)


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

    def test_as_custom_properties(self) -> None:
        node = Node(
            space="IntegrationTestsImmutable",
            external_id="shop:case:integration_test",
            version=1,
            type=DirectRelationReference("someSpace", "someType"),
            last_updated_time=123,
            created_time=123,
            deleted_time=None,
            properties=Properties(
                {
                    ViewId("power-models", "WindTurbine", "v1"): {
                        "name": "MyWindTurbine",
                        "wind_farm": "Utsira Nord",
                        "rotor": DirectRelationReference("space", "external_id"),
                    }
                }
            ),
        ).property_as_type(WindTurbine)

        assert isinstance(node.properties, WindTurbine)
        assert node.properties.name == "MyWindTurbine"
        assert node.properties.wind_farm == "Utsira Nord"
        assert node.properties.rotor == DirectRelationReference("space", "external_id")

        reloaded = Node.load(node.dump()).properties
        assert isinstance(reloaded, Properties)

    def test_node_list_as_custom_properties(self) -> None:
        nodes = NodeList(
            [
                Node(
                    space="IntegrationTestsImmutable",
                    external_id="shop:case:integration_test",
                    version=1,
                    type=DirectRelationReference("someSpace", "someType"),
                    last_updated_time=123,
                    created_time=123,
                    deleted_time=None,
                    properties=Properties(
                        {
                            ViewId("power-models", "WindTurbine", "v1"): {
                                "name": "MyWindTurbine",
                                "wind_farm": "Utsira Nord",
                                "rotor": DirectRelationReference("space", "external_id"),
                            }
                        }
                    ),
                ),
            ]
        ).property_as_type(WindTurbine)

        assert len(nodes) == 1
        for node in nodes:
            assert isinstance(node.properties, WindTurbine)
            assert node.properties.name == "MyWindTurbine"
            assert node.properties.wind_farm == "Utsira Nord"
            assert node.properties.rotor == DirectRelationReference("space", "external_id")


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


@pytest.mark.parametrize("instance_type", (Node, Edge))
def test_instances__quick_property_access_single_source(
    instance_type: type[Instance],
    node_dumped: dict[str, Any],
    edge_dumped: dict[str, Any],
) -> None:
    # Note: 'property' in this test refers to an instance property, not a Python property
    resource = {Node: node_dumped, Edge: edge_dumped}[instance_type]
    resource["properties"] = {"space": {"view/v8": {"prop1": 1, "prop2": "two", "3prop": [1, 2, 3]}}}
    inst = instance_type.load(resource)

    # Non-property should fail __getitem__ and __delitem__:
    with pytest.raises(KeyError):
        inst["space"]
    with pytest.raises(KeyError):
        del inst["space"]
    # ...but __setitem__ should work, Python mantra is "we are adults". Either way, the API will block all
    # reserved ones, and we don't want to keep a duplicate list up-to-date:
    assert "space" not in inst
    inst["space"] = "more-space"
    assert "space" in inst  # ensre __contains__ reflects change
    assert inst.space == "craft"  # ...ensure attribute not affected

    # Any property should work fine with all access/set/delete:
    assert inst.properties[ViewId("space", "view", "v8")]["prop1"] == 1
    inst["prop1"] = 123
    assert inst["prop1"] == 123
    assert inst.properties[ViewId("space", "view", "v8")]["prop1"] == 123
    del inst["prop1"]
    assert "prop1" not in inst.properties[ViewId("space", "view", "v8")]

    with pytest.raises(KeyError):
        inst["prop1"]

    assert inst.get("prop1") is None
    assert inst.get("prop1", "missing") == "missing"


@pytest.mark.parametrize("instance_type", (Node, Edge))
def test_instances__quick_property_access_no_source(
    instance_type: type[Instance],
    node_dumped: dict[str, Any],
    edge_dumped: dict[str, Any],
) -> None:
    # Note: 'property' in this test refers to an instance property, not a Python property
    resource = {Node: node_dumped, Edge: edge_dumped}[instance_type]
    resource["properties"] = {}
    inst = instance_type.load(resource)

    # Non-property should fail __[get/set/del]item__ because instance is not "single-sourced":
    with pytest.raises(RuntimeError):
        inst["space"]
    with pytest.raises(RuntimeError):
        inst["space"] = "more-space"
    with pytest.raises(RuntimeError):
        del inst["space"]
    with pytest.raises(RuntimeError):
        "space" in inst

    # ...same applies to properties. We have none so everything should fail:
    with pytest.raises(RuntimeError):
        inst["prop1"]  # get
    with pytest.raises(RuntimeError):
        inst["prop1"] = 1  # set
    with pytest.raises(RuntimeError):
        del inst["prop1"]  # del

    # ...even 'get':
    with pytest.raises(RuntimeError):
        inst.get("prop1")
    with pytest.raises(RuntimeError):
        inst.get("prop1", "missing")


@pytest.mark.dsl
class TestInstancesToPandas:
    @pytest.mark.parametrize("inst_cls", (Node, Edge))
    def test_expand_properties(
        self, node_dumped: dict[str, Any], edge_dumped: dict[str, Any], inst_cls: type[Node] | type[Edge]
    ) -> None:
        raw = {Node: node_dumped, Edge: edge_dumped}[inst_cls]
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

    @pytest.mark.parametrize("inst_cls", (Node, Edge))
    def test_expand_properties_empty_properties(
        self, node_dumped: dict[str, Any], edge_dumped: dict[str, Any], inst_cls: type[Node] | type[Edge]
    ) -> None:
        raw = {Node: node_dumped, Edge: edge_dumped}[inst_cls]
        raw["properties"] = {}
        expanded_with_empty_properties = inst_cls._load(raw).to_pandas(
            expand_properties=True, remove_property_prefix=True
        )

        assert "properties" not in expanded_with_empty_properties.index

    @pytest.mark.parametrize("inst_cls", (NodeList, EdgeList))
    def test_expand_properties__list_class(
        self, node_dumped: dict[str, Any], edge_dumped: dict[str, Any], inst_cls: type[NodeList] | type[EdgeList]
    ) -> None:
        raw = {NodeList: node_dumped, EdgeList: edge_dumped}[inst_cls]
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

    @pytest.mark.parametrize("inst_cls", (NodeList, EdgeList))
    def test_expand_properties__list_class_empty_properties(
        self, node_dumped: dict[str, Any], edge_dumped: dict[str, Any], inst_cls: type[NodeList] | type[EdgeList]
    ) -> None:
        raw = {NodeList: node_dumped, EdgeList: edge_dumped}[inst_cls]
        raw["properties"] = {}
        expanded_with_empty_properties = inst_cls._load([raw, raw]).to_pandas(
            expand_properties=True, remove_property_prefix=True
        )

        assert "properties" not in expanded_with_empty_properties.columns
