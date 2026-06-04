from __future__ import annotations

from collections.abc import Iterator

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Node, NodeApply, NodeId, NodeList, SpaceApply
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteDescribableNode, CogniteTimeSeriesApply
from cognite.client.utils._text import random_string

SPACE = "sp_python_sdk_dm_ts_tests"
EXT_ID_PREFIX = "pysdk-dmts-int-test"


def unique_id(description: str = "ts") -> str:
    return f"{EXT_ID_PREFIX}-{description}-{random_string(10)}"


@pytest.fixture(scope="session")
def dm_ts_space(cognite_client: CogniteClient) -> str:
    return cognite_client.data_modeling.spaces.apply(
        SpaceApply(
            SPACE,
            name="SDK DM Time Series Test Space",
            description="Space for testing time-series-related data modeling features",
        )
    ).space


@pytest.fixture(scope="session")
def ts_node(cognite_client: CogniteClient, dm_ts_space: str) -> Iterator[NodeId]:
    node = CogniteTimeSeriesApply(
        space=dm_ts_space,
        external_id=unique_id(),
        name="integration-test-ts",
        time_series_type="numeric",
        is_step=False,
    )
    cognite_client.data_modeling.instances.apply(node)
    node_id = node.as_id()
    yield node_id
    cognite_client.data_modeling.instances.delete(nodes=node_id)


@pytest.fixture(scope="session")
def non_ts_node(cognite_client: CogniteClient, dm_ts_space: str) -> Iterator[NodeId]:
    ext_id = unique_id("non-ts")
    cognite_client.data_modeling.instances.apply(nodes=NodeApply(space=dm_ts_space, external_id=ext_id))
    node_id = NodeId(dm_ts_space, ext_id)
    yield node_id
    cognite_client.data_modeling.instances.delete(nodes=node_id)


class TestRetrieve:
    def test_single_ts_node(self, cognite_client: CogniteClient, ts_node: NodeId) -> None:
        result = cognite_client.data_modeling.time_series.retrieve(ts_node)
        assert isinstance(result, Node)
        assert result.as_id() == ts_node

    def test_single_non_ts_node_returns_none(self, cognite_client: CogniteClient, non_ts_node: NodeId) -> None:
        result = cognite_client.data_modeling.time_series.retrieve(non_ts_node)
        assert result is None

    def test_multiple_mixed(self, cognite_client: CogniteClient, ts_node: NodeId, non_ts_node: NodeId) -> None:
        nonexistent = NodeId(SPACE, "i-do-not-exist-at-all")
        result = cognite_client.data_modeling.time_series.retrieve([ts_node, non_ts_node, nonexistent])
        assert isinstance(result, NodeList)
        assert len(result) == 1
        assert result[0].as_id() == ts_node

    def test_with_custom_source(self, cognite_client: CogniteClient, ts_node: NodeId) -> None:
        describable_view = CogniteDescribableNode.get_source()
        result = cognite_client.data_modeling.time_series.retrieve(ts_node, source=describable_view)
        assert isinstance(result, Node)
        assert result.as_id() == ts_node
        assert describable_view in result.properties
        assert CogniteTimeSeriesApply.get_source() not in result.properties
        assert result["name"] == "integration-test-ts"


class TestList:
    @pytest.mark.usefixtures("ts_node")
    def test_list_with_limit(self, cognite_client: CogniteClient) -> None:
        result = cognite_client.data_modeling.time_series.list(limit=3)
        assert isinstance(result, NodeList)
        assert 1 <= len(result) <= 3
