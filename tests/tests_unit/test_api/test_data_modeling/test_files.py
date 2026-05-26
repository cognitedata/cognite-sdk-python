from __future__ import annotations

import json
import re
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client._api.data_modeling.files import COGNITE_FILE_VIEW_ID, _resolve_source
from cognite.client.data_classes.data_modeling import Node, NodeId, NodeList, ViewId
from cognite.client.data_classes.data_modeling.instances import Properties
from tests.tests_unit.test_api.test_data_modeling.conftest import make_test_view

CUSTOM_VIEW_ID = ViewId("my-space", "MyView", "v1")


def single_node(
    space: str = "sp",
    external_id: str = "acex",
    extra_sources: dict[ViewId, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    props = Properties(
        {COGNITE_FILE_VIEW_ID: {"name": "a-file"}, **(extra_sources or {})},
    )
    return Node(
        space=space,
        external_id=external_id,
        version=1,
        last_updated_time=0,
        created_time=0,
        deleted_time=None,
        properties=props,
        type=None,
    ).dump(camel_case=True)


class TestResolveSource:
    @pytest.mark.parametrize(
        "source, expected_sources, expected_strip",
        [
            (COGNITE_FILE_VIEW_ID, [COGNITE_FILE_VIEW_ID], False),
            (("cdf_cdm", "CogniteFile", "v1"), [COGNITE_FILE_VIEW_ID], False),
            (CUSTOM_VIEW_ID, [CUSTOM_VIEW_ID, COGNITE_FILE_VIEW_ID], True),
            (("my-space", "MyView", "v1"), [CUSTOM_VIEW_ID, COGNITE_FILE_VIEW_ID], True),
            (make_test_view("my-space", "MyView", "v1"), [CUSTOM_VIEW_ID, COGNITE_FILE_VIEW_ID], True),
        ],
    )
    def test_resolve_source(self, source: Any, expected_sources: list[ViewId], expected_strip: bool) -> None:
        sources, strip = _resolve_source(source)
        assert sources == expected_sources
        assert strip is expected_strip

    def test_invalid_source_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Expected View, ViewId"):
            _resolve_source("not-a-valid-source")  # type: ignore[arg-type]


# avoid gzip compression in tests to simplify inspection of request bodies:
@pytest.mark.usefixtures("disable_gzip")
class TestDMFilesRetrieve:
    def test_default_source_sends_single_source_in_request(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/byids$"),
            json={"items": [single_node()]},
        )
        cognite_client.data_modeling.files.retrieve(NodeId("s", "x"))

        body = json.loads(httpx_mock.get_requests()[0].content)
        sources = body.get("sources", [])
        assert len(sources) == 1
        assert sources[0]["source"]["externalId"] == COGNITE_FILE_VIEW_ID.external_id

    def test_custom_source_sends_two_sources_and_strips_cognite_file_props(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/byids$"),
            json={"items": [single_node(extra_sources={CUSTOM_VIEW_ID: {"custom_prop": 42}})]},
        )
        result = cognite_client.data_modeling.files.retrieve(NodeId("s", "x"), source=CUSTOM_VIEW_ID)

        body = json.loads(httpx_mock.get_requests()[0].content)
        sources = body.get("sources", [])
        assert len(sources) == 2

        assert isinstance(result, Node)
        assert COGNITE_FILE_VIEW_ID not in result.properties
        assert CUSTOM_VIEW_ID in result.properties
        assert result["custom_prop"] == 42

    def test_single_id_not_found_returns_none(self, cognite_client: CogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/byids$"),
            json={"items": []},
        )
        result = cognite_client.data_modeling.files.retrieve(NodeId("s", "missing"))
        assert result is None

    def test_single_id_found_returns_node_not_list(self, cognite_client: CogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/byids$"),
            json={"items": [single_node()]},
        )
        result = cognite_client.data_modeling.files.retrieve(NodeId("s", "x"))
        assert isinstance(result, Node)

    def test_list_of_ids_returns_node_list(self, cognite_client: CogniteClient, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/byids$"),
            json={"items": [single_node("s", "xx"), single_node("s", "yy")]},
        )
        result = cognite_client.data_modeling.files.retrieve([NodeId("s", "xx"), NodeId("s", "yy")])
        assert isinstance(result, NodeList)
        assert len(result) == 2


@pytest.mark.usefixtures("disable_gzip")
class TestDMFilesList:
    def test_default_source_sends_single_source_in_request(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/list$"),
            json={"items": []},
        )
        cognite_client.data_modeling.files.list(limit=1)

        body = json.loads(httpx_mock.get_requests()[0].content)
        sources = body.get("sources", [])
        assert len(sources) == 1
        assert sources[0]["source"]["externalId"] == COGNITE_FILE_VIEW_ID.external_id

    def test_custom_source_sends_two_sources_and_strips_cognite_file_props(
        self, cognite_client: CogniteClient, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(r".*/models/instances/list$"),
            json={
                "items": [
                    single_node("s", "xx", extra_sources={CUSTOM_VIEW_ID: {"custom_prop": 42}}),
                    single_node("s", "yy", extra_sources={CUSTOM_VIEW_ID: {"custom_prop": 43}}),
                ]
            },
        )
        results = cognite_client.data_modeling.files.list(source=CUSTOM_VIEW_ID, limit=5)

        body = json.loads(httpx_mock.get_requests()[0].content)
        sources = body.get("sources", [])
        assert len(sources) == 2

        assert isinstance(results, NodeList)
        for node in results:
            assert COGNITE_FILE_VIEW_ID not in node.properties
            assert CUSTOM_VIEW_ID in node.properties
