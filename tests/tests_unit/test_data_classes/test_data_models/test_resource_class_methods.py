"""
This file contains tests for the methods for the various resource classes in
data modelling, like `NodeList.get`, `SpaceList.extend` etc.
"""

from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes._base import CogniteResourceList
from cognite.client.data_classes.data_modeling import EdgeId, EdgeList, Node, NodeId, NodeList, Space, SpaceList
from cognite.client.data_classes.data_modeling.instances import DataModelingInstancesList
from cognite.client.utils._identifier import InstanceId


@pytest.fixture
def instance_lst() -> list[dict[str, Any]]:
    return [
        {
            "space": f"foo{i}",
            "externalId": "constant",
            "version": 3 + i,
            "lastUpdatedTime": 1705935251161 + i,
            "createdTime": 1694600277822 + i,
            "properties": {"foo": {"Asset/v1": {"prop": i}}},
        }
        for i in range(3)
    ]


@pytest.fixture
def node_lst(instance_lst: list[dict[str, Any]]) -> NodeList:
    return NodeList([Node.load({"instanceType": "node", **inst}) for inst in instance_lst])


@pytest.fixture
def edge_lst(instance_lst: list[dict[str, Any]]) -> EdgeList:
    for inst in instance_lst:
        inst.update(
            instanceType="edge",
            type={"space": "foo", "externalId": "bar"},
            startNode={"space": "foo", "externalId": "bar2"},
            endNode={"space": "foo", "externalId": "bar3"},
        )
    return EdgeList.load(instance_lst)


@pytest.fixture
def space_lst(instance_lst: list[dict[str, Any]]) -> SpaceList:
    return SpaceList(
        [Space(space=dct["space"], is_global=False, last_updated_time=1, created_time=1) for dct in instance_lst]
    )


@pytest.mark.parametrize("space", ["foo0", "foo1", "foo2"])
@pytest.mark.parametrize("node_or_edge", ["node", "edge"])
def test_get_instance_lists(node_lst: NodeList, edge_lst: EdgeList, space: str, node_or_edge: str) -> None:
    inst_lst: DataModelingInstancesList = {"node": node_lst, "edge": edge_lst}[node_or_edge]  # type: ignore [assignment]

    for _id_cls in EdgeId, NodeId, InstanceId:  # pass by id object
        inst = inst_lst.get(_id_cls(space, "constant"))
        assert inst.space == space  # type: ignore [union-attr]

        assert inst_lst.get((space, "doesnt-exist")) is None

    inst = inst_lst.get((space, "constant"))  # pass by tuple
    assert inst.space == space  # type: ignore [union-attr]

    # Since ext.id is ambiguous, we always get the last (deprecated):
    inst = inst_lst.get(external_id="constant")
    assert inst.space == "foo2"  # type: ignore [union-attr]


@pytest.mark.parametrize("space", ["foo0", "foo1", "foo2"])
def test_get_space_list(space_lst: SpaceList, space: str) -> None:
    item = space_lst.get(space)
    assert item.space == space  # type: ignore [union-attr]

    assert space_lst.get(space + "doesnt-exist") is None


@pytest.mark.parametrize("which", ("space", "node", "edge"))
def test_extend_method(node_lst: NodeList, edge_lst: EdgeList, space_lst: SpaceList, which: str) -> None:
    lst: CogniteResourceList = {"node": node_lst, "edge": edge_lst, "space": space_lst}[which]  # type: ignore [assignment]
    empty = lst[:0]
    overlapping = lst[:]
    not_overlapping = lst[2:]
    lst = lst[:2]

    lst.extend(empty)
    lst.extend(not_overlapping)
    with pytest.raises(ValueError, match=r"^Unable to extend as this would introduce duplicates$"):
        lst.extend(overlapping)
