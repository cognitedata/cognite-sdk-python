from __future__ import annotations

from typing import Any

import pytest

from cognite.client._api.data_modeling.instances import InstancesAPI
from cognite.client.data_classes.data_modeling import Edge, Node
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import Instance
from tests.tests_unit.test_api.test_data_modeling.conftest import make_test_view

SINGLE_SRC_DUMP = {"source": {"space": "a", "externalId": "b", "version": "c", "type": "view"}}
SINGLE_SRC_DUMP_NO_VERSION = {"source": {"space": "a", "externalId": "b", "version": None, "type": "view"}}


@pytest.mark.parametrize(
    "sources, expected",
    (
        # Single
        (("a", "b", "c"), [SINGLE_SRC_DUMP]),
        (("a", "b"), [SINGLE_SRC_DUMP_NO_VERSION]),
        (ViewId("a", "b", "c"), [SINGLE_SRC_DUMP]),
        (ViewId("a", "b"), [SINGLE_SRC_DUMP_NO_VERSION]),
        (make_test_view("a", "b", "c"), [SINGLE_SRC_DUMP]),
        # Multiple
        ((("a", "b", "c"), ("a", "b", "c")), [SINGLE_SRC_DUMP, SINGLE_SRC_DUMP]),
        ([("a", "b", "c"), ("a", "b")], [SINGLE_SRC_DUMP, SINGLE_SRC_DUMP_NO_VERSION]),
        ((ViewId("a", "b"), ("a", "b", "c")), [SINGLE_SRC_DUMP_NO_VERSION, SINGLE_SRC_DUMP]),
        ([ViewId("a", "b"), ViewId("a", "b", "c")], [SINGLE_SRC_DUMP_NO_VERSION, SINGLE_SRC_DUMP]),
        (
            [make_test_view("a", "b", None), ViewId("a", "b", "c")],
            [SINGLE_SRC_DUMP_NO_VERSION, SINGLE_SRC_DUMP],
        ),
    ),
)
def test_instances_api_dump_instance_source(sources, expected):
    # We need to support:
    # ViewIdentifier = Union[ViewId, Tuple[str, str], Tuple[str, str, str]]
    # ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View]
    assert expected == InstancesAPI._dump_instance_source(sources)


@pytest.fixture
def empty_node_dump():
    return {
        "space": "space",
        "externalId": "xid",
        "version": 1,
        "lastUpdatedTime": 2,
        "createdTime": 3,
    }


@pytest.fixture
def empty_edge_dump():
    return {
        "space": "space",
        "externalId": "xid",
        "version": 4,
        "lastUpdatedTime": 5,
        "createdTime": 6,
        "type": {"space": "foo-space", "externalId": "bar-xid"},
        "startNode": {"space": "the-space", "externalId": "the-xid"},
        "endNode": {"space": "the-space", "externalId": "the-xid"},
    }


@pytest.mark.parametrize("instance_type", (Node, Edge))
def test_instances__quick_property_access_single_source(
    instance_type: type[Instance],
    empty_node_dump: dict[str, Any],
    empty_edge_dump: dict[str, Any],
):
    # Note: 'property' in this test refers to an instance property, not a Python property
    resource = {Node: empty_node_dump, Edge: empty_edge_dump}[instance_type]
    resource["properties"] = {"space": {"view/v8": {"prop1": 1, "prop2": "two", "3prop": [1, 2, 3]}}}
    inst = instance_type.load(resource)

    # Non-property should fail __getitem__ and __setitem__:
    with pytest.raises(KeyError):
        inst["space"]
    with pytest.raises(RuntimeError):
        inst["space"] = "more-space"

    assert hasattr(inst, "id") is False
    assert hasattr(inst, "space") is True
    assert inst.space == "space"
    inst.space = "more-space"
    assert inst.space == "more-space"

    # Property should work fine with both access/set/delete protocols:
    assert inst.prop1 == 1
    assert inst["prop1"] == 1

    inst.prop1 = 5
    assert inst["prop1"] == 5

    inst["prop1"] = 1
    assert inst.prop1 == 1

    del inst.prop1
    assert inst.get("prop1") is None
    assert inst.get("prop1", "missing") == "missing"
    with pytest.raises(KeyError):
        inst["prop1"]
    assert inst.prop2 == "two"
    del inst["prop2"]
    with pytest.raises(AttributeError):
        inst.prop2


@pytest.mark.parametrize("instance_type", (Node, Edge))
def test_instances__quick_property_access_no_source(
    instance_type: type[Instance],
    empty_node_dump: dict[str, Any],
    empty_edge_dump: dict[str, Any],
):
    # Note: 'property' in this test refers to an instance property, not a Python property
    resource = {Node: empty_node_dump, Edge: empty_edge_dump}[instance_type]
    resource["properties"] = {}
    inst = instance_type.load(resource)

    # Non-property should fail __getitem__ and __setitem__:
    with pytest.raises(RuntimeError):
        inst["space"]
    with pytest.raises(RuntimeError):
        inst["space"] = "more-space"

    assert hasattr(inst, "id") is False
    assert hasattr(inst, "space") is True
    assert inst.space == "space"
    inst.space = "more-space"
    assert inst.space == "more-space"

    # No source, so no properties. Everything should fail:
    with pytest.raises(AttributeError):
        inst.prop1
    with pytest.raises(RuntimeError):
        inst["prop1"]

    with pytest.raises(AttributeError):
        inst.prop1 = 5
    with pytest.raises(RuntimeError):
        inst["prop1"] = 1

    with pytest.raises(AttributeError):
        del inst.prop1
    with pytest.raises(RuntimeError):
        del inst["prop1"]
    with pytest.raises(RuntimeError):
        inst.get("prop1")
    with pytest.raises(RuntimeError):
        inst.get("prop1", "missing")
