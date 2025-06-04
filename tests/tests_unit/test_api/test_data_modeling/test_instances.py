from __future__ import annotations

import json
import math
import re

import pytest
import respx
from respx import MockRouter

from cognite.client import CogniteClient
from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.query import SourceSelector
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
                [make_test_view("a", "b", None), ViewId("a", "b", "c")],
                [SINGLE_SRC_DUMP_NO_VERSION, SINGLE_SRC_DUMP],
            ),
        ),
    )
    def test_instances_api_dump_instance_source(self, sources, expected):
        # We need to support:
        # ViewIdentifier = Union[ViewId, Tuple[str, str], Tuple[str, str, str]]
        # ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View]
        assert expected == [source.dump() for source in SourceSelector._load_list(sources)]


class TestAggregate:
    @respx.mock
    @pytest.mark.usefixtures("disable_gzip")
    @pytest.mark.parametrize("limit", [None, -1, math.inf])
    def test_aggregate_maximum(
        self, limit: int | float | None, respx_mock: MockRouter, cognite_client: CogniteClient
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
        respx_mock.post(url).respond(status_code=200, json=response)

        _ = cognite_client.data_modeling.instances.aggregate(
            ViewId("my_space", "MyView", "v1"), Count("externalId"), group_by="site", limit=limit
        )
        assert len(respx_mock.calls) == 1
        call = respx_mock.calls[0]
        body = json.loads(call.request.content) # respx uses request.content for bytes body
        assert "limit" in body
        assert body["limit"] == cognite_client.data_modeling.instances._AGGREGATE_LIMIT


class TestSearch:
    @respx.mock
    @pytest.mark.usefixtures("disable_gzip")
    @pytest.mark.parametrize("limit", [None, -1, math.inf])
    def test_search_maximum(self, limit: int | float | None, respx_mock: MockRouter, cognite_client: CogniteClient) -> None:
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
        respx_mock.post(url).respond(status_code=200, json=response)

        _ = cognite_client.data_modeling.instances.search(ViewId("my_space", "MyView", "v1"), "dummy text", limit=limit)
        assert len(respx_mock.calls) == 1
        call = respx_mock.calls[0]
        body = json.loads(call.request.content) # respx uses request.content for bytes body
        assert "limit" in body
        assert body["limit"] == cognite_client.data_modeling.instances._SEARCH_LIMIT
