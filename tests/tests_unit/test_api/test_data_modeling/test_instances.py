from __future__ import annotations

import json
import math
import re
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.query import (
    NodeResultSetExpressionSync,
    QueryResult,
    QuerySync,
    SelectSync,
    SourceSelector,
)
from cognite.client.data_classes.data_modeling.sync import SyncSessionWithCache
from tests.tests_unit.test_api.test_data_modeling.conftest import make_test_view

SINGLE_SRC_DUMP = {"source": {"space": "a", "externalId": "b", "version": "c", "type": "view"}}
SINGLE_SRC_DUMP_NO_VERSION = {"source": {"space": "a", "externalId": "b", "version": None, "type": "view"}}


class TestSourceDef:
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
                [make_test_view("a", "b", None), ViewId("a", "b", "c")],  # type: ignore[arg-type]
                [SINGLE_SRC_DUMP_NO_VERSION, SINGLE_SRC_DUMP],
            ),
        ),
    )
    def test_instances_api_dump_instance_source(self, sources: Any, expected: Any) -> None:
        # We need to support:
        # ViewIdentifier = Union[ViewId, Tuple[str, str], Tuple[str, str, str]]
        # ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View]
        assert expected == [source.dump() for source in SourceSelector._load_list(sources)]


class TestAggregate:
    @pytest.mark.usefixtures("disable_gzip")
    @pytest.mark.parametrize("limit", [None, -1, math.inf])
    def test_aggregate_maximum(
        self, limit: int | None, httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> None:
        url = re.compile(r".*/models/instances/aggregate$")
        response = {
            "items": [
                {
                    "instanceType": "node",
                    "group": {"site": "MyLocation"},
                    "aggregates": [
                        {
                            "aggregate": "count",
                            "property": "site",
                            "value": 42,
                        }
                    ],
                },
            ]
        }
        httpx_mock.add_response(method="POST", url=url, status_code=200, json=response)

        cognite_client.data_modeling.instances.aggregate(
            ViewId("my_space", "MyView", "v1"), Count("externalId"), group_by="site", limit=limit
        )
        assert len(httpx_mock.get_requests()) == 1
        req = httpx_mock.get_requests()[0]
        body = json.loads(req.content)
        assert "limit" in body
        assert body["limit"] == async_client.data_modeling.instances._AGGREGATE_LIMIT


class TestSearch:
    @pytest.mark.usefixtures("disable_gzip")
    @pytest.mark.parametrize("limit", [None, -1, math.inf])
    def test_search_maximum(
        self, limit: int | None, httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> None:
        url = re.compile(r".*/models/instances/search$")
        response = {
            "items": [
                {
                    "instanceType": "node",
                    "version": 1,
                    "space": "my_instance_space",
                    "externalId": "my_instance_id",
                    "createdTime": 0,
                    "lastUpdatedTime": 0,
                },
            ]
        }
        httpx_mock.add_response(method="POST", url=url, status_code=200, json=response)

        _ = cognite_client.data_modeling.instances.search(ViewId("my_space", "MyView", "v1"), "dummy text", limit=limit)
        assert len(httpx_mock.get_requests()) == 1
        req = httpx_mock.get_requests()[0]
        body = json.loads(req.content)
        assert "limit" in body
        assert body["limit"] == async_client.data_modeling.instances._SEARCH_LIMIT

    def test_search_using_invalid_operator(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="Invalid operator='INVALID'"):
            cognite_client.data_modeling.instances.search(
                ViewId("my_space", "MyView", "v1"),
                "dummy text",
                operator="INVALID",  # type: ignore [call-overload]
            )


class TestSyncSessionWithCache:
    @pytest.fixture
    def session(self) -> SyncSessionWithCache:
        query = QuerySync(
            with_={"movies": NodeResultSetExpressionSync(limit=10)},
            select={"movies": SelectSync([])},
        )
        return SyncSessionWithCache(
            api=MagicMock(),
            query=query,
            file_external_id="test_cache_file",
            security_category=42,
            backup_every=None,
            backup_on_exit=True,
        )

    def test_enter_raises_not_implemented_error(self, session: SyncSessionWithCache) -> None:
        with pytest.raises(NotImplementedError, match="async context manager"):
            with session:
                pass  # pragma: no cover

    @pytest.mark.parametrize(
        "counts, limits, expected",
        [
            # Single key
            ({"k": 10}, {"k": 10}, True),  # at limit → full
            ({"k": 11}, {"k": 10}, True),  # over limit → full (should not happen ofc...)
            ({"k": 9}, {"k": 10}, False),  # under limit → not full
            ({"k": 0}, {"k": 10}, False),  # empty → not full
            # Multiple keys: ANY key at limit makes the batch full
            ({"k1": 10, "k2": 10}, {"k1": 10, "k2": 10}, True),  # both full
            ({"k1": 10, "k2": 9}, {"k1": 10, "k2": 10}, True),  # one full, one not
            ({"k1": 9, "k2": 9}, {"k1": 10, "k2": 10}, False),  # ALL under limit → not full
        ],
    )
    def test_batch_is_full(self, counts: dict, limits: dict, expected: bool) -> None:
        query = QuerySync(
            with_={k: NodeResultSetExpressionSync(limit=v) for k, v in limits.items()},
            select={k: SelectSync([]) for k in limits},
        )
        sess = SyncSessionWithCache(
            api=MagicMock(),
            query=query,
            file_external_id="f",
            security_category=0,
            backup_every=None,
            backup_on_exit=True,
        )
        result = QueryResult({k: [None] * v for k, v in counts.items()})
        assert sess._batch_is_full(result) is expected

    def test_compute_query_hash_strips_cursors(self, session: SyncSessionWithCache) -> None:
        hash_without = SyncSessionWithCache._compute_query_hash(session._query)
        session._query.cursors = {"movies": "some_cursor_value"}
        hash_with = SyncSessionWithCache._compute_query_hash(session._query)
        assert hash_without == hash_with

    def test_get_nodes_raises_on_edge_data(self, session: SyncSessionWithCache) -> None:
        session._instances = {"movies": [{"instanceType": "edge", "space": "s", "externalId": "e"}]}
        with pytest.raises(ValueError, match="edges"):
            session.get_nodes("movies")

    def test_get_edges_raises_on_node_data(self, session: SyncSessionWithCache) -> None:
        session._instances = {"movies": [{"instanceType": "node", "space": "s", "externalId": "e"}]}
        with pytest.raises(ValueError, match="nodes"):
            session.get_edges("movies")
