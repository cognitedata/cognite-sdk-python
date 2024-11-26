from __future__ import annotations

from datetime import date, datetime
from typing import Any, cast

import pytest

from cognite.client.data_classes.data_modeling import (
    ContainerId,
    DirectRelationReference,
    Edge,
    EdgeApply,
    EdgeList,
    Float64,
    Node,
    NodeApply,
    NodeId,
    NodeList,
    NodeListWithCursor,
    NodeOrEdgeData,
    ViewId,
)
from cognite.client.data_classes.data_modeling.data_types import UnitReference
from cognite.client.data_classes.data_modeling.instances import (
    EdgeListWithCursor,
    Instance,
    TypeInformation,
    TypePropertyDefinition,
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
            "startNode": {
                "space": "mySpace",
                "externalId": "person.external_id",
            },
            "endNode": {"space": "mySpace", "externalId": "actor.external_id"},
        }

    def test_dump_camel_case_false(self) -> None:
        edge = EdgeApply(
            space="mySpace",
            external_id="relation:arnold_schwarzenegger:actor",
            type=DirectRelationReference("mySpace", "Person.role"),
            start_node=DirectRelationReference("mySpace", "source"),
            end_node=DirectRelationReference("mySpace", "target"),
        )

        assert edge.dump(camel_case=False) == {
            "space": "mySpace",
            "external_id": "relation:arnold_schwarzenegger:actor",
            "instance_type": "edge",
            "type": {
                "space": "mySpace",
                "external_id": "Person.role",
            },
            "start_node": {
                "space": "mySpace",
                "external_id": "source",
            },
            "end_node": {"space": "mySpace", "external_id": "target"},
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
                        date=date(2024, 6, 18),
                        start_time=datetime.fromisoformat("2021-01-01T00:00:00"),
                        end_time=datetime.fromisoformat("2021-01-01T00:00:00"),
                        other_nodes=cast(
                            list[NodeId | DirectRelationReference],
                            [
                                DirectRelationReference("space", "external_id"),
                                NodeId("space", "external_id2"),
                            ],
                        ),
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
                        "end_time": "2021-01-01T00:00:00.000",
                        "name": "Integration test",
                        "runStatus": "Running",
                        "scenario": "Integration test",
                        "start_time": "2021-01-01T00:00:00.000",
                        "date": "2024-06-18",
                        "other_nodes": [
                            {"externalId": "external_id", "space": "space"},
                            {"externalId": "external_id2", "space": "space"},
                        ],
                    },
                    "source": {"externalId": "Case", "space": "IntegrationTestsImmutable", "type": "container"},
                }
            ],
            "space": "IntegrationTestsImmutable",
            "type": {"externalId": "someType", "space": "someSpace"},
        }

    def test_dump_camel_case_false(self) -> None:
        node = NodeApply(
            space="IntegrationTestsImmutable",
            external_id="shop:case:integration_test",
        )

        assert node.dump(camel_case=False) == {
            "external_id": "shop:case:integration_test",
            "instance_type": "node",
            "space": "IntegrationTestsImmutable",
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


class TestNodeListWithCursor:
    def test_extend(self) -> None:
        default_args: dict[str, Any] = dict(
            version=1, last_updated_time=0, created_time=0, deleted_time=None, properties=None, type=None
        )
        nodes = NodeListWithCursor[Node](
            [
                Node(space="space", external_id="node1", **default_args),
                Node(space="space", external_id="node2", **default_args),
            ],
            cursor="original_cursor",
        )

        nodes.extend(
            NodeListWithCursor[Node](
                [
                    Node(space="space", external_id="node3", **default_args),
                    Node(space="space", external_id="node4", **default_args),
                ],
                cursor="next_cursor",
            ),
        )
        assert len(nodes) == 4
        assert nodes.cursor == "next_cursor"

    def test_extend_with_duplicates(self) -> None:
        default_args: dict[str, Any] = dict(
            version=1, last_updated_time=0, created_time=0, deleted_time=None, properties=None, type=None
        )
        nodes = NodeListWithCursor[Node](
            [
                Node(space="space", external_id="node1", **default_args),
                Node(space="space", external_id="node2", **default_args),
            ],
            cursor="original_cursor",
        )

        with pytest.raises(ValueError):
            nodes.extend(
                NodeListWithCursor[Node](
                    [
                        Node(space="space", external_id="node2", **default_args),
                        Node(space="space", external_id="node3", **default_args),
                    ],
                    cursor="next_cursor",
                ),
            )


class TestEdgeListWithCursor:
    def test_extend(self) -> None:
        default_args: dict[str, Any] = dict(
            start_node=DirectRelationReference("space", "node1"),
            end_node=DirectRelationReference("space", "node2"),
            version=1,
            last_updated_time=0,
            created_time=0,
            deleted_time=None,
            properties=None,
            type=DirectRelationReference("space", "type"),
        )
        edges = EdgeListWithCursor(
            [
                Edge(space="space", external_id="edge1", **default_args),
                Edge(space="space", external_id="edge2", **default_args),
            ],
            cursor="original_cursor",
        )

        edges.extend(
            EdgeListWithCursor(
                [
                    Edge(space="space", external_id="edge3", **default_args),
                    Edge(space="space", external_id="edge4", **default_args),
                ],
                cursor="next_cursor",
            ),
        )
        assert len(edges) == 4
        assert edges.cursor == "next_cursor"

    def test_extend_with_duplicates(self) -> None:
        default_args: dict[str, Any] = dict(
            start_node=DirectRelationReference("space", "node1"),
            end_node=DirectRelationReference("space", "node2"),
            version=1,
            last_updated_time=0,
            created_time=0,
            deleted_time=None,
            properties=None,
            type=DirectRelationReference("space", "type"),
        )
        edges = EdgeListWithCursor(
            [
                Edge(space="space", external_id="edge1", **default_args),
                Edge(space="space", external_id="edge2", **default_args),
            ],
            cursor="original_cursor",
        )

        with pytest.raises(ValueError):
            edges.extend(
                EdgeListWithCursor(
                    [
                        Edge(space="space", external_id="edge2", **default_args),
                        Edge(space="space", external_id="edge3", **default_args),
                    ],
                    cursor="next_cursor",
                ),
            )


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
def node_overlapping_props() -> dict[str, Any]:
    return {
        "space": "sp",
        "externalId": "copy pastarino",
        "version": 22,
        "lastUpdatedTime": 123456789,
        "createdTime": 17326520,
        "instanceType": "node",
        "properties": {
            "my-space": {
                "my-view/v8": {
                    "name": "huh A NAME",
                    "type": "numeric",
                    "lastUpdatedTime": "not a number (🐫)",
                    "last_updated_time": "not a number (🐍)",
                    "unit": {"space": "cdf_cdm_units", "externalId": "temperature:deg_c"},
                    "externalId": "overlaps-ext-id",
                }
            }
        },
        "type": {"space": "asdf", "externalId": "im-a-time-series-type"},
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

    def test_instances_to_pandas_expand_true_with_overlapping_props(
        self, node_dumped: dict, node_overlapping_props: dict
    ) -> None:
        # We don't test Edge/EdgeList as it uses the same to_pandas implementation as NodeList
        nodes: NodeList = NodeList.load([node_dumped, node_overlapping_props])
        node = Node.load(node_overlapping_props)
        # We need to test with camel_case T/F as e.g. base prop. lastUpdatedTime only overlaps when camel_case=True
        # but user properties do not vary with camel_case
        kw: dict = {"expand_properties": True, "convert_timestamps": False}
        without_prefix_camel_lst = nodes.to_pandas(remove_property_prefix=True, camel_case=True, **kw)
        without_prefix_snake_lst = nodes.to_pandas(remove_property_prefix=True, camel_case=False, **kw)
        with_prefix_camel_lst = nodes.to_pandas(remove_property_prefix=False, camel_case=True, **kw)
        with_prefix_snake_lst = nodes.to_pandas(remove_property_prefix=False, camel_case=False, **kw)

        without_prefix_camel = node.to_pandas(remove_property_prefix=True, camel_case=True, **kw)
        without_prefix_snake = node.to_pandas(remove_property_prefix=True, camel_case=False, **kw)
        with_prefix_camel = node.to_pandas(remove_property_prefix=False, camel_case=True, **kw)
        with_prefix_snake = node.to_pandas(remove_property_prefix=False, camel_case=False, **kw)

        # Base property should never be affected:
        prefix = "my-space.my-view/v8"
        for df in without_prefix_camel_lst, without_prefix_snake_lst, with_prefix_camel_lst, with_prefix_snake_lst:
            base_type = df.at[1, "type"]
            assert isinstance(base_type, dict) and base_type["space"] == "asdf"
            assert df.at[1, f"{prefix}.type"] == "numeric"

        for df in without_prefix_camel, without_prefix_snake, with_prefix_camel, with_prefix_snake:
            base_type = df.at["type", "value"]
            assert isinstance(base_type, dict) and base_type["space"] == "asdf"
            assert df.at[f"{prefix}.type", "value"] == "numeric"

        # Check a base property that varies with camel_case:
        assert without_prefix_camel_lst.at[1, "lastUpdatedTime"] == 123456789
        assert without_prefix_snake_lst.at[1, "last_updated_time"] == 123456789
        assert with_prefix_camel_lst.at[1, "lastUpdatedTime"] == 123456789
        assert with_prefix_snake_lst.at[1, "last_updated_time"] == 123456789

        assert without_prefix_camel.at["lastUpdatedTime", "value"] == 123456789
        assert without_prefix_snake.at["last_updated_time", "value"] == 123456789
        assert with_prefix_camel.at["lastUpdatedTime", "value"] == 123456789
        assert with_prefix_snake.at["last_updated_time", "value"] == 123456789

        # Check the overlapping user property
        assert without_prefix_camel_lst.at[1, "last_updated_time"] == "not a number (🐍)"
        assert without_prefix_snake_lst.at[1, "lastUpdatedTime"] == "not a number (🐫)"
        assert without_prefix_camel_lst.at[1, f"{prefix}.lastUpdatedTime"] == "not a number (🐫)"
        assert without_prefix_snake_lst.at[1, f"{prefix}.last_updated_time"] == "not a number (🐍)"

        assert without_prefix_camel.at["last_updated_time", "value"] == "not a number (🐍)"
        assert without_prefix_snake.at["lastUpdatedTime", "value"] == "not a number (🐫)"
        assert without_prefix_camel.at[f"{prefix}.lastUpdatedTime", "value"] == "not a number (🐫)"
        assert without_prefix_snake.at[f"{prefix}.last_updated_time", "value"] == "not a number (🐍)"

        for df in with_prefix_camel_lst, with_prefix_snake_lst:
            for prop_name, case in zip(("last_updated_time", "lastUpdatedTime"), ("🐍", "🐫")):
                assert df.at[1, f"{prefix}.{prop_name}"] == f"not a number ({case})"
                # Ensure the property doesn't exist without prefix - or that it's the base
                # property when camel_case aligns with base prop.
                assert prop_name not in df.columns or df.at[1, prop_name] == 123456789

        for df in with_prefix_camel, with_prefix_snake:
            for prop_name, case in zip(("last_updated_time", "lastUpdatedTime"), ("🐍", "🐫")):
                assert df.at[f"{prefix}.{prop_name}", "value"] == f"not a number ({case})"
                # Ensure the property doesn't exist without prefix - or that it's the base
                # property when camel_case aligns with base prop.
                assert prop_name not in df.index or df.at[prop_name, "value"] == 123456789

    def test_node_with_single_property_to_pandas_with_expand_props(self) -> None:
        # Bug prior to 7.62.6 made to_pandas(expand_properties=True) fail on nodes with a single property
        # due to how squeeze works in pandas, even a DataFrame will be forced into a scalar (to be fair,
        # the documentation is very clear on this).
        node = Node.load(
            {
                "space": "DEMO_AppData",
                "externalId": "15777214-1234-4321-1234-02f7b4cd349d",
                "version": 888,
                "lastUpdatedTime": 1694611301244,
                "createdTime": 1685430260401,
                "instanceType": "node",
                "properties": {"DEMO_AppData": {"APM_User/15": {"name": "Foo Bar"}}},
            }
        )
        # This has always worked, and should continue to work after the fix
        node_df1 = node.to_pandas(expand_properties=False)
        assert node_df1.at["properties", "value"] == {"DEMO_AppData": {"APM_User/15": {"name": "Foo Bar"}}}

        # This failed prior to 7.62.6:
        node_df2 = node.to_pandas(expand_properties=True)
        assert "properties" not in node_df2.index
        assert node_df2.at["name", "value"] == "Foo Bar"


class TestTypeInformation:
    @pytest.mark.dsl
    def test_to_pandas(self) -> None:
        import pandas as pd

        info = TypeInformation(
            {
                "my_space": {
                    "view_id/v1": {
                        "pressure": TypePropertyDefinition(
                            type=Float64(unit=UnitReference(external_id="pressure:pa")),
                            nullable=True,
                            auto_increment=False,
                        ),
                    }
                }
            }
        )
        expected = pd.DataFrame.from_dict(
            {
                ("my_space", "view_id/v1"): {
                    "identifier": "pressure",
                    "type.list": False,
                    "type.unit.external_id": "pressure:pa",
                    "type.type": "float64",
                    "nullable": True,
                    "autoIncrement": False,
                    "defaultValue": None,
                    "name": None,
                    "description": None,
                    "immutable": False,
                }
            },
            orient="index",
        )
        expected.index.names = "space_name", "view_or_container"

        df = info.to_pandas()

        pd.testing.assert_frame_equal(df, expected)
